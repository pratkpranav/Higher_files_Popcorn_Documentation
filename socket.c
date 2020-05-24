
/**
 * msg_socket.c
 *  Messaging transport layer over TCP/IP
 *
 * Authors:
 *  Ho-Ren (Jack) Chuang <horenc@vt.edu>
 *  Sang-Hoon Kim <sanghoon@vt.edu>
 */
#include <linux/seq_file.h>
#include <linux/proc_fs.h>
#include <linux/kthread.h>
#include <popcorn/stat.h>
#include "ring_buffer.h"
#include "common.h"
#define PORT 30467
#define MAX_SEND_DEPTH	1024
#define NIPQUAD(addr) ((unsigned char *)&addr)[0],((unsigned char *)&addr)[1],((unsigned char *)&addr)[2],((unsigned char *)&addr)[3]
#define NIPQUAD_FMT "%u.%u.%u.%u"

enum {
	SEND_FLAG_POSTED = 0,
};

struct q_item {
	struct pcn_kmsg_message *msg;
	unsigned long flags;
	struct completion *done;
};

/* Per-node handle for socket */
struct sock_handle {
	int nid;

	/* Ring buffer for queueing outbound messages */
	struct q_item *msg_q;
	unsigned long q_head;
	unsigned long q_tail;
	spinlock_t q_lock;
	struct semaphore q_empty;
	struct semaphore q_full;

	struct socket *sock;
	struct task_struct *send_handler;
	struct task_struct *recv_handler;
};
static struct sock_handle sock_handles[MAX_NUM_NODES] = {};

static struct socket *sock_listen = NULL;
static struct ring_buffer send_buffer = {};


/**
 * Handle inbound messages
 */
/*declares struct and receives kernel message*/
static int ksock_recv(struct socket *sock, char *buf, size_t len)
{
	struct msghdr msg = {
		.msg_flags = 0,
		.msg_control = NULL,
		.msg_controllen = 0,
		.msg_name = NULL,
		.msg_namelen = 0,
	};
	struct kvec iov = {
		.iov_base = buf,
		.iov_len = len,
	};

	return kernel_recvmsg(sock, &msg, &iov, 1, len, MSG_WAITALL);
}


/*receives a message, sends it to kernal 
then further send it to pcn_kmsg_proces*/
static int recv_handler(void* arg0)
{
	struct sock_handle *sh = arg0;
	MSGPRINTK("RECV handler for %d is ready\n", sh->nid);

	while (!kthread_should_stop()) {
		int len;
		int ret;
		size_t offset;
		struct pcn_kmsg_hdr header;
		char *data;

		/* compose header */
		offset = 0;
		len = sizeof(header);
		while (len > 0) {
			ret = ksock_recv(sh->sock, (char *)(&header) + offset, len);
			if (ret == -1) break;
			offset += ret;
			len -= ret;
		}
		if (ret < 0) break;

#ifdef CONFIG_POPCORN_CHECK_SANITY
		BUG_ON(header.type < 0 || header.type >= PCN_KMSG_TYPE_MAX);
		BUG_ON(header.size < 0 || header.size >  PCN_KMSG_MAX_SIZE);
#endif

		/* compose body */
		data = kmalloc(header.size, GFP_KERNEL);
		BUG_ON(!data && "Unable to alloc a message");

		memcpy(data, &header, sizeof(header));

		offset = sizeof(header);
		len = header.size - offset;

		while (len > 0) {
			ret = ksock_recv(sh->sock, data + offset, len);
			if (ret == -1) break;
			offset += ret;
			len -= ret;
		}
		if (ret < 0) break;

		/* Call pcn_kmsg upper layer */
		pcn_kmsg_process((struct pcn_kmsg_message *)data);
	}
	return 0;
}


/**
 * Handle outbound messages
 */

/*declares struct and send message to kernel*/
static int ksock_send(struct socket *sock, char *buf, size_t len)
{
	struct msghdr msg = {
		.msg_flags = 0,
		.msg_control = NULL,
		.msg_controllen = 0,
		.msg_name = NULL,
		.msg_namelen = 0,
	};
	struct kvec iov = {
		.iov_base = buf,
		.iov_len = len,
	};

	return kernel_sendmsg(sock, &msg, &iov, 1, len);
}

/*add message in the queue*/
static int enq_send(int dest_nid, struct pcn_kmsg_message *msg, unsigned long flags, struct completion *done)
{
	int ret;
	unsigned long at;
	struct sock_handle *sh = sock_handles + dest_nid;
	struct q_item *qi;
	do {
		ret = down_interruptible(&sh->q_full);
	} while (ret);

	spin_lock(&sh->q_lock);
	at = sh->q_tail;
	qi = sh->msg_q + at;
	sh->q_tail = (at + 1) & (MAX_SEND_DEPTH - 1);

	qi->msg = msg;
	qi->flags = flags;
	qi->done = done;
	spin_unlock(&sh->q_lock);
	up(&sh->q_empty);

	return at;
}

void sock_kmsg_put(struct pcn_kmsg_message *msg);

/*take out message from the queue*/
static int deq_send(struct sock_handle *sh)
{
	int ret;
	char *p;
	unsigned long from;
	size_t remaining;
	struct pcn_kmsg_message *msg;
	struct q_item *qi;
	unsigned long flags;
	struct completion *done;

	do {
		ret = down_interruptible(&sh->q_empty);
	} while (ret);

	spin_lock(&sh->q_lock);
	from = sh->q_head;
	qi = sh->msg_q + from;
	sh->q_head = (from + 1) & (MAX_SEND_DEPTH - 1);

	msg = qi->msg;
	flags = qi->flags;
	done = qi->done;
	spin_unlock(&sh->q_lock);
	up(&sh->q_full);

	p = (char *)msg;
	remaining = msg->header.size;

	while (remaining > 0) {
		int sent = ksock_send(sh->sock, p, remaining);
		if (sent < 0) {
			MSGPRINTK("send interrupted, %d\n", sent);
			io_schedule();
			continue;
		}
		p += sent;
		remaining -= sent;
		//printk("Sent %d remaining %d\n", sent, remaining);
	}
	if (test_bit(SEND_FLAG_POSTED, &flags)) {
		sock_kmsg_put(msg);
	}
	if (done) complete(done);

	return 0;
}

/*send sock_handler for receiving message 
and frees the memory*/
static int send_handler(void* arg0)
{
	struct sock_handle *sh = arg0;
	MSGPRINTK("SEND handler for %d is ready\n", sh->nid);

	while (!kthread_should_stop()) {
		deq_send(sh);
	}
	kfree(sh->msg_q);
	return 0;
}


#define WORKAROUND_POOL
/***********************************************
 * Manage send buffer
 ***********************************************/

/*a message is taken fromt he ring_buffer 
and returned*/
struct pcn_kmsg_message *sock_kmsg_get(size_t size)
{
	struct pcn_kmsg_message *msg;
	might_sleep();

#ifdef WORKAROUND_POOL
	msg = kmalloc(size, GFP_KERNEL);
#else
	while (!(msg = ring_buffer_get(&send_buffer, size))) {
		WARN_ON_ONCE("ring buffer is full\n");
		schedule();
	}
#endif
	return msg;
}


/*to put the message in the send_buffer*/
void sock_kmsg_put(struct pcn_kmsg_message *msg)
{
#ifdef WORKAROUND_POOL
	kfree(msg);
#else
	ring_buffer_put(&send_buffer, msg);
#endif
}


/***********************************************
 * This is the interface for message layer
 ***********************************************/

/*send message to enq_send function in order 
to send the message to the kernel*/
int sock_kmsg_send(int dest_nid, struct pcn_kmsg_message *msg, size_t size)
{
	DECLARE_COMPLETION_ONSTACK(done);
	enq_send(dest_nid, msg, 0, &done);

	if (!try_wait_for_completion(&done)) {
		int ret = wait_for_completion_io_timeout(&done, 60 * HZ);
		if (!ret) return -EAGAIN;
	}
	return 0;
}


/*calls above function*/
int sock_kmsg_post(int dest_nid, struct pcn_kmsg_message *msg, size_t size)
{
	enq_send(dest_nid, msg, 1 << SEND_FLAG_POSTED, NULL);
	return 0;
}

/*frees the message memory*/
void sock_kmsg_done(struct pcn_kmsg_message *msg)
{
	kfree(msg);
}

/*print stats*/
void sock_kmsg_stat(struct seq_file *seq, void *v)
{
	if (seq) {
		seq_printf(seq, POPCORN_STAT_FMT,
				(unsigned long long)ring_buffer_usage(&send_buffer),
#ifdef CONFIG_POPCORN_STAT
				(unsigned long long)send_buffer.peak_usage,
#else
				0ULL,
#endif
                                "socket");
	}
}

struct pcn_kmsg_transport transport_socket = {
	.name = "socket",
	.features = 0,

	.get = sock_kmsg_get,
	.put = sock_kmsg_put,
	.stat = sock_kmsg_stat,

	.send = sock_kmsg_send,
	.post = sock_kmsg_post,
	.done = sock_kmsg_done,
};


/*print infos about the connected IPs*/
static int __show_peers(struct seq_file *seq, void *v)
{	
	int i;
	char* myself = " ";	
	for (i = 0; i < MAX_NUM_NODES; i++) 
	{
		if (i == my_nid) myself = "*";
		seq_printf(seq, "%s %3d  "NIPQUAD_FMT"  %s\n",
				myself,
                                i,
                                NIPQUAD(ip_table[i]),
                                "NODE_IP");
		myself = " ";
	}
	return 0;
}

/*saves a file with info from __show_peers*/
static int __open_peers(struct inode *inode, struct file *file)
{
        return single_open(file, &__show_peers, NULL);
}



static struct file_operations peers_ops = {
        .owner = THIS_MODULE,
        .open = __open_peers,
        .read = seq_read,
        .llseek  = seq_lseek,
        .release = single_release,
};
static struct proc_dir_entry *proc_entry = NULL;

/*creates a file popcorn_peers under the 
/proc directory*/
static int peers_init(void)
{
	proc_entry = proc_create("popcorn_peers",  0444, NULL, &peers_ops);
        if (proc_entry == NULL) {
                printk(KERN_ERR"cannot create proc_fs entry for popcorn stats\n");
                return -ENOMEM;
        }
        return 0;
}

/*creates and run a 
thread with assigned parameters*/
static struct task_struct * __init __start_handler(const int nid, const char *type, int (*handler)(void *data))
{
	char name[40];
	struct task_struct *tsk;

	sprintf(name, "pcn_%s_%d", type, nid);
	tsk = kthread_run(handler, sock_handles + nid, name);
	if (IS_ERR(tsk)) {
		printk(KERN_ERR "Cannot create %s handler, %ld\n", name, PTR_ERR(tsk));
		return tsk;
	}

	/* TODO: support prioritized msg handler
	struct sched_param param = {
		sched_priority = 10};
	};
	sched_setscheduler(tsk, SCHED_FIFO, &param);
	set_cpus_allowed_ptr(tsk, cpumask_of(i%NR_CPUS));
	*/
	return tsk;
}



/*starts two thread one for sending 
and receiving tasks and assign both the
thread to the sock_handler*/
static int __start_handlers(const int nid)
{
	struct task_struct *tsk_send, *tsk_recv;
	tsk_send = __start_handler(nid, "send", send_handler);
	if (IS_ERR(tsk_send)) {
		return PTR_ERR(tsk_send);
	}

	tsk_recv = __start_handler(nid, "recv", recv_handler);
	if (IS_ERR(tsk_recv)) {
		kthread_stop(tsk_send);
		return PTR_ERR(tsk_recv);
	}
	sock_handles[nid].send_handler = tsk_send;
	sock_handles[nid].recv_handler = tsk_recv;
	return 0;
}


/*
creates socket and connects to kernel 
and then calls start handler
*/
static int __init __connect_to_server(int nid)
{
	int ret;
	struct sockaddr_in addr;
	struct socket *sock;

	ret = sock_create(PF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
	if (ret < 0) {
		MSGPRINTK("Failed to create socket, %d\n", ret);
		return ret;
	}

	addr.sin_family = AF_INET;
	addr.sin_port = htons(PORT);
	addr.sin_addr.s_addr = ip_table[nid];

	MSGPRINTK("Connecting to %d at %pI4\n", nid, ip_table + nid);
	do {
		ret = kernel_connect(sock, (struct sockaddr *)&addr, sizeof(addr), 0);
		if (ret < 0) {
			MSGPRINTK("Failed to connect the socket %d. Attempt again!!\n", ret);
			msleep(1000);
		}
	} while (ret < 0);

	sock_handles[nid].sock = sock;
	ret = __start_handlers(nid);
	if (ret) return ret;

	return 0;
}

/*initialize a nid process and 
then calls start handler*/
static int __init __accept_client(int *nid)
{
	int i;
	int ret;
	int retry = 0;
	bool found = false;
	struct socket *sock;
	struct sockaddr_in addr;
	int addr_len = sizeof(addr);

	do {
		ret = sock_create(PF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
		if (ret < 0) {
			MSGPRINTK("Failed to create socket, %d\n", ret);
			return ret;
		}

		ret = kernel_accept(sock_listen, &sock, 0);
		if (ret < 0) {
			MSGPRINTK("Failed to accept, %d\n", ret);
			goto out_release;
		}

		ret = kernel_getpeername(sock, (struct sockaddr *)&addr, &addr_len);
		if (ret < 0) {
			goto out_release;
		}

		/* Identify incoming peer nid */
		for (i = 0; i < MAX_NUM_NODES; i++) {
			if (addr.sin_addr.s_addr == ip_table[i]) {
				*nid = i;
				found = true;
			}
		}
		if (!found) {
			sock_release(sock);
			continue;
		}
	} while (retry++ < 10 && !found);

	if (!found) return -EAGAIN;
	sock_handles[*nid].sock = sock;

	ret = __start_handlers(*nid);
	if (ret) goto out_release;

	return 0;

out_release:
	sock_release(sock);
	return ret;
}


/*finds a connection*/
static int __init __listen_to_connection(void)
{
	int ret;
	struct sockaddr_in addr;

	ret = sock_create(PF_INET, SOCK_STREAM, IPPROTO_TCP, &sock_listen);
	if (ret < 0) {
		printk(KERN_ERR "Failed to create socket, %d", ret);
		return ret;
	}

	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(PORT);

	ret = kernel_bind(sock_listen, (struct sockaddr *)&addr, sizeof(addr));
	if (ret < 0) {
		printk(KERN_ERR "Failed to bind socket, %d\n", ret);
		goto out_release;
	}

	ret = kernel_listen(sock_listen, MAX_NUM_NODES);
	if (ret < 0) {
		printk(KERN_ERR "Failed to listen to connections, %d\n", ret);
		goto out_release;
	}

	MSGPRINTK("Ready to accept incoming connections\n");
	return 0;

out_release:
	sock_release(sock_listen);
	sock_listen = NULL;
	return ret;
}


/*free each memory and stop the threads*/
static void __exit exit_kmsg_sock(void)
{
	int i;

	if (sock_listen) sock_release(sock_listen);

	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct sock_handle *sh = sock_handles + i;
		if (sh->send_handler) {
			kthread_stop(sh->send_handler);
		} else {
			if (sh->msg_q) kfree(sh->msg_q);
		}
		if (sh->recv_handler) {
			kthread_stop(sh->recv_handler);
		}
		if (sh->sock) {
			sock_release(sh->sock);
		}
	}
	ring_buffer_destroy(&send_buffer);

	MSGPRINTK("Successfully unloaded module!\n");
}


/*initialize the TCP/IP network between 
the messaging layer*/
static int __init init_kmsg_sock(void)
{
	int i, ret;

	MSGPRINTK("Loading Popcorn messaging layer over TCP/IP...\n");

	if (!identify_myself()) return -EINVAL;
	pcn_kmsg_set_transport(&transport_socket);

	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct sock_handle *sh = sock_handles + i;

		sh->msg_q = kmalloc(sizeof(*sh->msg_q) * MAX_SEND_DEPTH, GFP_KERNEL);
		if (!sh->msg_q) {
			ret = -ENOMEM;
			goto out_exit;
		}

		sh->nid = i;
		sh->q_head = 0;
		sh->q_tail = 0;
		spin_lock_init(&sh->q_lock);

		sema_init(&sh->q_empty, 0);
		sema_init(&sh->q_full, MAX_SEND_DEPTH);
	}

	if ((ret = ring_buffer_init(&send_buffer, "sock_send"))) goto out_exit;

	if ((ret = __listen_to_connection())) return ret;

	/* Wait for a while so that nodes are ready to listen to connections */
	msleep(100);

	/* Initilaize the sock.
	 *
	 *  Each node has a connection table like tihs:
	 * --------------------------------------------------------------------
	 * | connect | connect | (many)... | my_nid(one) | accept | (many)... |
	 * --------------------------------------------------------------------
	 * my_nid:  no need to talk to itself
	 * connect: connecting to existing nodes
	 * accept:  waiting for the connection requests from later nodes
	 */
	for (i = 0; i < my_nid; i++) {
		if ((ret = __connect_to_server(i))) goto out_exit;
		set_popcorn_node_online(i, true);
	}

	set_popcorn_node_online(my_nid, true);

	for (i = my_nid + 1; i < MAX_NUM_NODES; i++) {
		int nid;
		if ((ret = __accept_client(&nid))) goto out_exit;
		set_popcorn_node_online(nid, true);
	}

	broadcast_my_node_info(i);

	PCNPRINTK("Ready on TCP/IP\n");
	peers_init();
	
	return 0;

out_exit:
	exit_kmsg_sock();
	return ret;
}

module_init(init_kmsg_sock);
module_exit(exit_kmsg_sock);
MODULE_LICENSE("GPL");
