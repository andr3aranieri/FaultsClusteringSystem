# FaultsClusteringSystem

## Introduction

A fault is essentially a text description of what’s not working as it should.
The goal of this project is to try to estimate the number of cluster in a dinamically populated set of faults. Essentially i’ll try to estimate the K in K-Means.
The initial estimate is done using the Silhouette coefficient (https://en.wikipedia.org/wiki/Silhouette_(clustering)). 
Starting from this, the FaultsClustering Hadoop job will be iterated to evaluate if the actual K-Clustering is natural. 

## System Architecture

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/SystemArchitecture.png)

The basic idea is to implement a StreamReader server which lets users to create new source streams from which read raw data. 
Raw data are problem descriptions: they will be preprocessed and mapped to a N-Dimensional tfidf Euclidean space (N = number of terms).

In this project i implemented 2 **StreamReader** modules:
- **TwitterReader**: it downloads a twitter stream filtered by account (account mentions and direct messages), location, or keywords. Thie can be used to mine the whole twitter to detect information abount him (search “Telco name” + “sms” + “problem” in a certain location);
- **MailReader**: it periodically downloads mail from a configured imap account. This can be used to intercept mails sent by customers to notify problems.

These 2 modules will send received data to the **TextProcessing** module (remove punctuation, remove stopwords, lemming,...) which will save formatted docs in datastore: each doc will have the following structure:
** * *doc_i\t tfidf_i1 tfidf_i2 tfidf_i3 tfidf_i3 ….  tfidf_iN* * ** 
* *tfidf_ij, 1 <= i <= M, 1 <= j <= N* * (N: num terms in the document collection; M: num docs in document collection)

I’ll represent each doc with a vector in the N-Dimensional tfidf euclidean space.
Documents distance will be chosen after some test; it is configurable (clustering modules could choose a distance measure in function of the clustering they need).
For online clustering, space dimension will grow each time we receive docs with new terms.
For offline clustering the space dimension could be not efficiently storable in memory, depending on the chosen time period of anal
ysis.
To control memory usage i use 
- real tfidf space until a certain dimension **N_max** is reached;
- an estimated N_max dimension space after **N_max**. The estimated tfidf space uses a hash function to hash terms in a fixed number of buckets (**N_max** buckets): 2 terms that hash to the same bucket, will have the same estimated tfidf;

The **StreamReader** is a python multithreading batch program: each new stream is executed in a different thread.

There are be 2 datastores:
- **persistent data store** that will store docs for a certain period (for example a year);
- **polatile data store** that will store daily docs;

Stored docs will be clusterized by 2 clustering modules:
-- **historical clustering**: clusterizes docs in a specified time period;
-- **on line clustering**: clusterizes the daily collected docs;

The 2 clustering modules will be implemented with hadoop jobs. 
The number of clusters will be estimated by clustering modules.

The “Show Clusters” RESTfull web service will be implemented in a future release. It will be used by external monitoring / notification systems (e.g. when a new cluster (a new problem) is created, the system will notify it in real time to the managers that will take the necessary actions to find a solution).

## StreamReader Module

To start the module, from “StreamerModuleFolder”:

`python StartStreamerModule.py`

It’s a python multithreaded module which contains 2 different modules:
- TwitterReader;
- MailReader;

It has a Stream Socket interface listening on port number 8899 that can be tested using telnet

`telnet 127.0.0.1 8899`

The command has this structure:

`op:param1=value1|param2=value2|...|paramN=valueN##STOP##`


### Twitter Reader

It reads a twitter stream of any of these kinds:
- Direct Messages and mentions of a specified account;
- Public tweets filtered by keywords and location;

For public tweets are provided the following filters:
- **locations**: the bounding box which indicates the area from which we want to receive tweets. es.  ‘12.341707,41.769596,12.730289,42.050546'
- **track**: keywords. They can be provided
  - separated by space to obtain tweets with all the keywords. es “vodafone not receiving sms”;
  - separated by comma to obtain tweets with any of the keyword. es “vodafone, 4g slow, internet connection problem”;

A command example to create a public twitter stream is:

`twtr_pub:idUser=1|loc=12.341707,41.769596,12.730289,42.050546|kw=LTE VODAFONE##STOP##`

To create a private twitter stream, the user create a twitter app connected to his account and pass its configuration parameters to the streamer module. The command will have the following form:

`twtr_pri:idUser=1|cku=****************|csu=*******************|atu=**************|atus=****************##STOP##`

where 
- **cku**: comsumer key;
- **csu**: consumer secret;
- **atu**: access token;
- **atus**: access token secret;

### Mail Reader

This module implements an imap client that periodically downloads mails from the configured account.
It can be configured on a single imap folder from which to download mails. The default value is ‘INBOX’.
A command example to create a mail stream is:

`mail_ima:idUser=1|server=imap.gmail.com|userName=user_mail@mail.com|password=user_password|folder=ticket##STOP##`

To avoid duplicates, after downloading the UNSEEN emails in the configured folder, it sets their flag to SEEN. A future improvement of the algorithm will be to avoid this not to alter human user mail inbox interation.

### Closing a stream

The command to close a stream:

`clos_str:23##STOP##`

closes the stream with id 23.

### Streamer Module Structure
```
./Module
./Module/__init__.py
./Module/BusinessLogic
./Module/BusinessLogic/__init__.py
./Module/BusinessLogic/MailReader.py
./Module/BusinessLogic/TextPreprocessManager.py
./Module/BusinessLogic/ThreadDispatcher.py
./Module/BusinessLogic/TwitterReader.py
./Module/ModuleInterface
./Module/ModuleInterface/__init__.py
./Module/ModuleInterface/StreamerServer.py
./Module/ResourceManagementLayer
./Module/ResourceManagementLayer/__init__.py
./Module/ResourceManagementLayer/DB
./Module/ResourceManagementLayer/DB/__init__.py
./Module/ResourceManagementLayer/DB/DBConfig.py
./Module/ResourceManagementLayer/DB/DBManager.py
./Module/ResourceManagementLayer/DB/PersistentDocumentDB.py
./Module/ResourceManagementLayer/DB/StreamDB.py
./Module/ResourceManagementLayer/DB/VolatileDocumentDB.py
./Module/ResourceManagementLayer/Entity
./Module/ResourceManagementLayer/Entity/__init__.py
./Module/ResourceManagementLayer/Entity/PersistentDocument.py
./Module/ResourceManagementLayer/Entity/Stream.py
./Module/ResourceManagementLayer/Entity/VolatileDocument.py
./Module/ResourceManagementLayer/Manager
./Module/ResourceManagementLayer/Manager/__init__.py
./Module/ResourceManagementLayer/Manager/DocumentManager.py
./Module/ResourceManagementLayer/Manager/StreamManager.py
./StartStreamerModule.py
```

The StreamerModule is a 3-tier python application:

1. **ModuleInterface**: provides a TCP socket interface (configurable port, default = 8899);
2. **BusinessLogic**: contains the logic needed to parse a client request, to start and end a stream, to preprocess received documents;
3. **ResourceManagementLayer**: contains the logic to access datastore:
  * * *DB submodule* *: direct access to the particular datastore manager (MySQL DBMS in this case);
  * * *Entity submodule* *: logical representation of datastore entities;
  * * *Manger submodule* *: interface to datastore for business logic module;
  
## DataBase

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/db.png)

User table is used to adapt the system to different users needs: each user can create his own streams and the system will cluster his documents. It contains user clustering configuration:
- distance measure chosen for this user;
- k value, the estimated number of clusters for a certain user. This value is updated by the system as explained next in the document;
- clustering state, used by java hadoop jobs that compute clustering for the user: using this value, we can return to user consistent clustering informations when he requests them (through web service invocation);

The **Term** table contains the term vocabulary for documents of a specific user. Fixing a user value, the term table number of rows determines the **VolatileDocument** and **PersistentDocument** vectors dimension. Depending on this dimension the system will decide to use the real documents dimension or to hash terms to contain memory usage.

**TmpCluster** table is used in hadoop K estimation job: a thread periodically computes clustering simulations on user’s daily data, using user’s actual K value, and stores results in TmpCluster table.

This clustering simulations are evalued by a hadoop **Silhouette** job, which uses Distance table to compute Silhouette coefficient of each point in this clustering: if overall Silhouette coefficient is good enough (boolean method that decides it), K value is OK, and the real user clustering will be done with K.

Initial K estimation is computed as follows

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/initialKEstimate.png)

To be able to execute hadoop Silhouette job efficiently, Distance table is updated periodically by a thread DistanceMeasureThread launched by java FaultsClusteringSystem application.

## Fault Clustering System

It’s the clustering application. It’s presented as an Eclipse project compiled as a jar executed in Hadoop.

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/faultclusteringproject.png)

To execute the application:

`hadoop jar FaultsClusteringSystem.jar faultsclusteringsystem.gui.StartSystem`

The application is a 3 thread process. In the following image the main method:

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/fault_mainmethod.png)


### DistanceMeasureThread

Periodically computes distances between each documents couple iterating twice documents space * * (O(N^2)) * * . The algorithm is very expensive, so the execution frequency can be decided in function to the number N of documents, modifiying the thread sleep time between 2 executions.

During the 2 annidated iterations, i use a hashmap to compute  **dist(doc1, doc2)** only once, considering that **dist(doc1, doc2)** = **dist(doc2, doc1)**. The key used in the hashmap is 

`doc1.id + “:” + doc2.id`

The computation output is a bulk insert query in the form:

`INSERT INTO Distance(idUser, distance, hasBeenRead, point1, point2) VALUES (1, 0.00123, 0, doc1ID, doc2ID),(1, 0.01313, 0, doc1ID, doc3ID),....`

to be able to efficiently write distances to DB.

Insert operation is done atomically together with delete of distances for this user:

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/insertdistances.png)


### KEstimation Algorithm

It’s implemented by the following method. It makes a clustering simulation with the actual K value, and estimates if it’s a natural clustering.

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/kestimation.png)

It works on a copy of volatile datastore, realized by db tables `TmpDocument` and `TmpCluster`

It’s divided in 3 steps:
1. index creation of today documents;
2. kmeans on today documents using index created;
3. evaluation of silhouette index of points in computed clusters, to determine if actual K value is good enough.

The output of this algorithm is a list of silhouette values for clustered points:

`s(i) = (b(i) - a(i)) / max(a(i), b(i)) (silhouette value for doc i)`

where

`a(i) = avg distance of i from other points in its cluster`
`b(i) = min distance between i and points in other clusters`

If  most of the points have a silhouette near to value 1, the clustering is natural, so we use the actual K value for real clustering (the clustering returned to users). Otherwise, we make another clustering simulation with K+1.


### User Clustering Algorithm

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/uca1.png)

**Index Creation**

This method creates inverted index matrix taking in input documents as list of terms. It uses 2 hadoop jobs to:

1. compute tfidf coefficient for terms;
2. create inverted index matrix;

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/uca2.png)

The output of the index creation hadoop job has to be written to DB: job reducer writes a list of SQL updates command 

![alt tag](https://github.com/andr3aranieri/FaultsClusteringSystem/blob/master/doc_resources/uca3.png)

that are atomically written to DB with a bulk update.

### KMeans

It’s a classical map reduce KMeans implementation. It’s implemented in 2 hadoop jobs:
1. select k centers;
2. iteration job to compute clusters till convergence to a treshold:
  * mapper assigns documents to k selected center;
  * reducer computes new k centers and decides to stop iterations if they differ from old ones less than the specified treshold;





















