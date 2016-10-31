# FaultsClusteringSystem

A clustering application for faults clustering using an iterative application of KMeans algorithm. The application is presented as a jar to execute in Hadoop. In this first release, faults are retrieved by the python StreamReader module and stored in the MySQL DB. There are 2 available sources: imap mail account and twitter stream. My intention is to be able to retrieve faults from other sources, and to give a RESTful API interface to let external application to populate the faults DB.
