### orientdb-jetty

Simple library for store HTTP session from Jetty in OrientDB.

#### Usage
1. Use configs from folder config/
2. After initialize application and acquire database call this:
    ```java
       OrientDbJettyModule.initSchema(ODatabaseDocument);
    ```
3. That's all.

#### Schema description
| Name                    | Description                                                                    |
|-------------------------|--------------------------------------------------------------------------------|
| OSessionData            | Class which contain all information about HTTP session                         |
| OSessionData.id         | String property which contains id of HTTP session provided byt Jetty           |
| OSessionData.data       | Binary property which contains serialized HTTP session data                    |
| OSessionData.expiryTime | Long property which contains time in milliseconds when session will be expired |

