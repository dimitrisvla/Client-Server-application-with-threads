/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/


#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"

#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10
#define NUM_THREADS		  10 	// Consumer threads number
#define QUEUE_SIZE		  25	// Queue size 


int inform_consumer = 0;	// Inform the Consumers that Producer is done! 
pthread_mutex_t inform_consumer_mutex;

// Definition of a queue element.
typedef struct element {
  int fd;
  struct timeval starting_time;
} Element;

Element queue[QUEUE_SIZE];	// Buffer is a circular queue.
int head = 0;			// head pointer 
int tail = 0;			// tail pointer
pthread_mutex_t queue_mutex;	// Protects head, tail, num_elements, queue.
pthread_cond_t condc, condp;    // Cond. variables for Synchronizing Producer-Consumers.

int completed_requests = 0;
double total_waiting_time;	 // total time inside queue.
double total_service_time;       // total time for processing every request.
pthread_t threads[NUM_THREADS];  // we need to join inside "ctr_z_handler()".
pthread_mutex_t request_mutex;	 // Protects the above variables.

int reader_count = 0;		 // unlimited
int writer_count = 0;		 // at most one
pthread_mutex_t read_write_mutex;
pthread_cond_t cond_store;       // we need a mutex and a cond.var. for Synchronizing Readers-Writer.

// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;

// Definition of the database.
KISSDB *db = NULL;


/**
 * &name check_error - Check if there is an error. Application terminates if necessary.
 * @param error_code - the error code
 * @param message - the message that should be displayed 
 */
void check_error(int error_code, char *message) {

  if (error_code != 0) {
    printf("%s\n", message);
    exit(EXIT_FAILURE);
  }
}


/**
 * @name calculate_num_elements() - Calculates how many elements are in Queue.
 * @return - Returns the number of elements.
 */
int calculate_num_elements() {

  int num_elements;

  if (head > tail) {
    num_elements = QUEUE_SIZE - (head - tail);
  } 
  else {
    num_elements = tail - head;
  }
	
  return(num_elements);
}


void calculate_total_w_time(struct timeval start_t, struct timeval end_t) {

  double waiting_time;

  //waiting_time = (end_t.tv_sec - start_t.tv_sec) + ((end_t.tv_usec - start_t.tv_usec)/1000000.0);
  pthread_mutex_lock(&request_mutex);
  waiting_time = (end_t.tv_sec - start_t.tv_sec) + ((end_t.tv_usec - start_t.tv_usec)/1000000.0);
  total_waiting_time = total_waiting_time + waiting_time;
  pthread_mutex_unlock(&request_mutex);
}

void calculate_total_s_time(struct timeval start_s, struct timeval end_s) {

  double service_time;

  //service_time = (end_s.tv_sec - start_s.tv_sec) + ((end_s.tv_usec - start_s.tv_usec)/1000000.0);
  pthread_mutex_lock(&request_mutex);
  service_time = (end_s.tv_sec - start_s.tv_sec) + ((end_s.tv_usec - start_s.tv_usec)/1000000.0);
  total_service_time = total_service_time + service_time;
  pthread_mutex_unlock(&request_mutex);
}


/**
 * @name enqueue() - Inserts an element into queue.
 */
void enqueue(int fd){

  pthread_mutex_lock(&queue_mutex);
  queue[tail].fd = fd;  			    // insert new fd
  gettimeofday(&queue[tail].starting_time, NULL);   // insert starting time
  tail = (tail + 1) % QUEUE_SIZE;		    // tail value always between 0 and QUEUE_SIZE - 1
  pthread_mutex_unlock(&queue_mutex);
}

/**
 * @name dequeue() - Removes an element from queue.
 * @return - Returns that element.
 */
Element dequeue(){

  struct timeval end_waiting;		// we'll calculate total waiting time here...
  Element temp_element;

  pthread_mutex_lock(&queue_mutex);
  gettimeofday(&end_waiting, NULL);
  calculate_total_w_time(queue[head].starting_time, end_waiting);	
  temp_element = queue[head];
  head = (head + 1) % QUEUE_SIZE; 	// head value always between 0 and QUEUE_SIZE-1
  pthread_mutex_unlock(&queue_mutex);

  return(temp_element);
}


// kill -SIGTSTP 17133
void ctr_z_handler() {

  double average_waiting_time;
  double average_service_time;
  int i;

  pthread_mutex_lock(&inform_consumer_mutex);
  inform_consumer = 1;
  pthread_mutex_unlock(&inform_consumer_mutex);

  pthread_mutex_lock(&queue_mutex);
  pthread_cond_broadcast(&condc);
  pthread_mutex_unlock(&queue_mutex);

  for (i=0; i<NUM_THREADS;i++) {
    pthread_join(threads[i], NULL);
  }

  // Compute the average times...
  average_waiting_time = (double)total_waiting_time / completed_requests;
  average_service_time = (double)total_service_time / completed_requests;
 
  printf("STATISTICS:\n");
  printf("\tCompleted Requests: %d\n",completed_requests);
  printf("\tAverage waiting time : %f seconds\n", average_waiting_time);
  printf("\tAverage service time : %f seconds\n", average_service_time);

  exit(EXIT_FAILURE);
}


/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}


/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
void process_request(const int socket_fd) {
  char response_str[BUF_SIZE], request_str[BUF_SIZE];
    int numbytes = 0;
    Request *request = NULL;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);

    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);

    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
        switch (request->operation) {
          case GET: // Reader code here...
            pthread_mutex_lock(&read_write_mutex);
            // while there is still a writer inside the store wait...
            while (writer_count > 0) {
              pthread_cond_wait(&cond_store, &read_write_mutex);
            }
            // Increment readers count inside a lock...
            reader_count++;
            pthread_mutex_unlock(&read_write_mutex);

            // Read the given key from the store.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);

            pthread_mutex_lock(&read_write_mutex);
	    reader_count--;
            pthread_cond_broadcast(&cond_store);
            pthread_mutex_unlock(&read_write_mutex);
            break;
          case PUT: // Writer code here...
            pthread_mutex_lock(&read_write_mutex);
            // while there are still readers or a writer inside the store wait...
            while((reader_count > 0) || (writer_count > 0)) {
              pthread_cond_wait(&cond_store, &read_write_mutex);
            }
            writer_count++;

	    // Write the given key/value pair to the store.
            if (KISSDB_put(db, request->key, request->value))
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");

	    writer_count--;
            pthread_cond_signal(&cond_store);
            pthread_mutex_unlock(&read_write_mutex);
            break;
          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));
        if (request)
          free(request);
        request = NULL;
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, response_str, strlen(response_str));
}


void* Worker(void *id){	
	
  Element new_element;
  int new_request_fd;		// Each time we receive a new fd we'll process a new request.
  struct timeval start_service, end_service;				
  //const int myid = (long)id;	// Force the pointer to be a 64-bit integer.

  while(1) {
 
    pthread_mutex_lock(&queue_mutex);
    // Check repeatedly whether queue is empty...
    while (calculate_num_elements() == 0) {
      //printf("WORKER THREAD %d: Queue is empty....\n", myid);
      if (inform_consumer == 1) {
        pthread_mutex_unlock(&queue_mutex);
	return NULL;
      }
      pthread_cond_wait(&condc, &queue_mutex);	
      if (inform_consumer == 1) {
        pthread_mutex_unlock(&queue_mutex);
	return NULL;
      }
    }

    new_element = dequeue();
    gettimeofday(&start_service, NULL);		// get time before processing.
    pthread_cond_signal(&condp);	 	// wake up the producer. CORRECT

    new_request_fd = new_element.fd;
    process_request(new_request_fd);		// process new request.

    gettimeofday(&end_service, NULL);		// get time after processing.
    pthread_mutex_unlock(&queue_mutex);		// release the buffer.

    calculate_total_s_time(start_service, end_service); // compute total service time.

    pthread_mutex_lock(&request_mutex);
    completed_requests++;			// we just completed a request.
    pthread_mutex_unlock(&request_mutex);

    close(new_request_fd);
  }
  return NULL;
}


/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {

  int i,
      error_init = 0;
  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information

  pthread_mutexattr_t Attr;
  pthread_mutexattr_init(&Attr);
  pthread_mutexattr_settype(&Attr, PTHREAD_MUTEX_RECURSIVE);

  error_init = pthread_mutex_init(&queue_mutex, &Attr);
  check_error(error_init, "Error: Can't initialize queue mutex...\n");
  error_init = pthread_cond_init(&condp, NULL);
  check_error(error_init, "Error: Can't initialize condition variable 'condp'...\n");
  error_init = pthread_cond_init(&condc, NULL);
  check_error(error_init, "Error: Can't initialize condition variable 'condc'...\n");

  error_init = pthread_mutex_init(&read_write_mutex, NULL);
  check_error(error_init, "Error: Can't initialize read write mutex...\n");
  error_init = pthread_cond_init(&cond_store, NULL);
  check_error(error_init, "Error: Can't initialize condition variable 'cond_store'...\n");
  
  error_init = pthread_mutex_init(&request_mutex, NULL);
  check_error(error_init, "Error: Can't initialize request mutex...\n");

  // Do we need it ?
  error_init = pthread_mutex_init(&inform_consumer_mutex, NULL);
  check_error(error_init, "Error: Can't initialize inform_consumer_mutex...\n");

  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);
  // Signal handler for Ctrl+Z
  signal(SIGTSTP, ctr_z_handler);
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }

  // Loop to create & start the Consumer threads.
  for (i=0; i<=NUM_THREADS; i++) {
    error_init = pthread_create(&threads[i], NULL, Worker, (void*)(long)i);
    check_error(error_init, "Error: Can't create consumer threads...\n");
  }

  // main loop: wait for new connection/requests
  while (1) { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }
    
    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));
    
     // We are about to enter the critical secction.
    pthread_mutex_lock(&queue_mutex);
    // Check repeatedly whether queue is full...
    while (calculate_num_elements() == QUEUE_SIZE - 1) {
      //printf("MAIN THREAD: Queue is full.....\n");
      pthread_cond_wait(&condp, &queue_mutex);
    }

    //printf("MAIN THREAD: new fd %d produced\n", new_fd);
    enqueue(new_fd);

    //pthread_cond_broadcast(&condc);
    pthread_cond_signal(&condc);	            // wake up a consumer
    pthread_mutex_unlock(&queue_mutex);		    // release the buffer
  }  

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

