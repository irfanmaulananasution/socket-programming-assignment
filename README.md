# Socket Programming Assignment
Computer Networking assignment of A6 team

### Description / Overview
This project is a jobs management system build from scratch to practice our socket programming lesson

### Requirements 
general requirement :
- Java
- two or more device to act as client and server (cloud machine/server, virtual macine, actual device)others : check requirements.txt

### Installation / How to run
to reuse this project do:
1. git clone the repository
2. run Worker.java in other device (worker can be run in more than one device)
  '''
  javac Worker.java
  java Worker <port for connection to master> (ex : java Worker 3333)
  '''
3. run Master.java in one device (master can only be run in one device)
  '''
  javac Master.java
  java Master
  '''
  the program will ask an input of the worker information with format <address>:<port>. put all the worker information. and program are ready to go


### Author
- Irfan Maulana Nasution
- Muhammad Yoga Mahendra
- Nasywa Nur Fathiyah

### License
[MIT](./LICENSE.txt)