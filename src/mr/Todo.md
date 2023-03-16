- There's one coordinator, and many worker being created in mr* script.
- Worker should run indefintely till coordinator send the "Done" task.
- Coordinator:
  [-] Parse the file names and store them in a array.
  [-] When received worker saying it is idle. 
    [-] If there are map tasks that not yet assigned:
      [-] Send the a map task containing the file name to the worker.
    - Else check whether if there's a task is taking too long
      - If so reissue the task to the worker
      - else send wait task
  - When worker finishes and received worker's complete status, 
    - record the list of nReduce intermediate filenames.
    - remove the originally mapped filename from map array.
    - send new map task if there's any
  - When all of the map tasks are finished, create reduce tasks passing each nReduce intermediate filenames to worker.
  - When worker finishes ...
  - When all of the reduce tasks are finished, send done message to each worker.
  - Shutdown

- Worker:
  [-] When wake up, send a IDLE message to coordinator 
  [-] After receiving a task
    [-] if the task is map
      - call the map function and store the result file names in the args intermediatefiles.
      - Send Done message to coordinator
    - if the task is reduce
      - call the reduce function and store the result ...
      - Send Done message to coordinator
    - if the tasks is wait
      - wait for 2 seconds and then message again
  


- Questions:
  - Does my logic work?
  - When does synchronization come in, in my implementation there's no need of locks
    - Potential answer: RPC Message is being handled concurrently, lock the shared array that stores the filenames.
  - How does the corrdinator directlty reachout to workers, or periodically check the runtime of workers. Can server do background tasks without receiving message?
  - What is the corrdinator in the function header