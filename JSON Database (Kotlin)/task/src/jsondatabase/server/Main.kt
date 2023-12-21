package jsondatabase.server

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import java.io.*
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

const val filePath = "./src/jsondatabase/server/db.json"
val reentrantReadWriteLock = ReentrantReadWriteLock()
var running = true
var jsonDb: JsonObject = JsonObject()
val gson = Gson()

class ClientHandler(clientSocket: Socket?) : Runnable {
    private var socket: Socket? = null

    init {
        try {
            socket = clientSocket
        } catch (ex: Exception) {
            ex.printStackTrace()
        }
    }

    override fun run() {
        try {
            val input = DataInputStream(socket!!.getInputStream())
            val output = DataOutputStream(socket!!.getOutputStream())

            val clientRequest = input.readUTF()
            println("Received: $clientRequest")

            val jsonRequest = gson.fromJson(clientRequest, JsonObject::class.java)

            val requestType = jsonRequest.get("type").asString
            val requestKey = jsonRequest.get("key")

            val response = JsonObject()

            when (requestType){
                "set" -> {
                    reentrantReadWriteLock.readLock().lock()
                    try {
                        val dbString = deserialize(filePath) as String
                        jsonDb = gson.fromJson(dbString, JsonObject::class.java)
                        println("DB loaded")
                    } catch (e: IOException) {
                        e.printStackTrace()
                    } catch (e: ClassNotFoundException) {
                        e.printStackTrace()
                    } finally {
                        reentrantReadWriteLock.readLock().unlock()
                    }

                    if (requestKey.isJsonArray) {
                        traverseAndModify(requestKey.asJsonArray, jsonRequest.get("value"))
                    } else {
                        jsonDb.add(requestKey.asString, jsonRequest.get("value"))
                    }

                    reentrantReadWriteLock.writeLock().lock()
                    try {
                        serialize(jsonDb.toString(), filePath)
                        response.addProperty("response", "OK")
                    } catch (e: IOException) {
                        e.printStackTrace()
                        response.addProperty("response", "ERROR")
                    } finally {
                        reentrantReadWriteLock.writeLock().unlock()
                    }
                }
                "get" -> {
                    reentrantReadWriteLock.readLock().lock()
                    try {
                        val dbString = deserialize(filePath) as String
                        jsonDb = gson.fromJson(dbString, JsonObject::class.java)
                        println("DB loaded")
                    } catch (e: IOException) {
                        e.printStackTrace()
                    } catch (e: ClassNotFoundException) {
                        e.printStackTrace()
                    } finally {
                        reentrantReadWriteLock.readLock().unlock()
                    }

                    reentrantReadWriteLock.readLock().lock()
                    // check for a JsonArray key
                    val jsonValue: JsonElement?
                    if (requestKey.isJsonArray) {
                        jsonValue = getNestedJsonValue(requestKey.asJsonArray)
                        if (jsonValue == null) {
                            response.addProperty("response", "ERROR")
                            response.addProperty("reason", "No such key")
                        } else {
                            response.addProperty("response", "OK")
                            response.add("value", jsonValue)
                        }
                    } else {
                        // otherwise String key
                        if (jsonDb.has(requestKey.asString)) {
                            response.addProperty("response", "OK")
                            response.addProperty("value", jsonDb.get(requestKey.asString).asString)
                        } else {
                            response.addProperty("response", "ERROR")
                            response.addProperty("reason", "No such key")
                        }
                    }
                    reentrantReadWriteLock.readLock().unlock()
                }
                "delete" -> {
                    reentrantReadWriteLock.readLock().lock()
                    try {
                        val dbString = deserialize(filePath) as String
                        jsonDb = gson.fromJson(dbString, JsonObject::class.java)
                        println("DB loaded")
                    } catch (e: IOException) {
                        e.printStackTrace()
                    } catch (e: ClassNotFoundException) {
                        e.printStackTrace()
                    } finally {
                        reentrantReadWriteLock.readLock().unlock()
                    }

                    if (requestKey.isJsonArray) {
                        reentrantReadWriteLock.writeLock().lock()
                        try {
                            if (deleteKey(jsonRequest.getAsJsonArray("key"))) {
                                response.addProperty("response", "OK")
                            } else {
                                response.addProperty("response", "ERROR")
                                response.addProperty("reason", "No such key")
                            }
                            serialize(jsonDb.toString(), filePath)
                        } finally {
                            reentrantReadWriteLock.writeLock().unlock()
                        }
                    } else {
                        // String key
                        reentrantReadWriteLock.writeLock().lock()
                        try {
                            if (jsonDb.has(requestKey.asString)) {
                                jsonDb.remove(requestKey.asString)
                                response.addProperty("response", "OK")
                            } else {
                                response.addProperty("response", "ERROR")
                                response.addProperty("reason", "No such key")
                            }

                            serialize(jsonDb.toString(), filePath)
                            println("DB written")
                        } catch (e: IOException) {
                            e.printStackTrace()
                        } finally {
                            reentrantReadWriteLock.writeLock().unlock()
                        }
                    }
                }
                "exit" -> {
                    response.addProperty("response", "OK")
                    running = false
                }
            }
            val jsonResponse = gson.toJson(response)
            output.writeUTF(jsonResponse)
            println("Sent: $response")
            socket!!.close()
        } catch (ex: Exception) {
            ex.printStackTrace()
        }
    }
}

fun getNestedJsonValue(complexKeyPath: JsonArray): JsonElement? {
    var currentElement: JsonElement = jsonDb
    for (keyElement in complexKeyPath) {
        println("Key: $keyElement")
        if (currentElement.isJsonObject) {
            val key = keyElement.asString
            if ((currentElement as JsonObject).has(key)) {
                println("Key '$key' found")
            } else {
                println("Key '$key' not found")
            }
            currentElement = currentElement[key]
            println("Current element: $currentElement")
        } else {
            return null
        }
    }
    return currentElement
}

fun traverseAndModify(
    complexKeyPath: JsonArray,
    newValue: JsonElement?
) {
    val keys = arrayOfNulls<String>(complexKeyPath.size())
    for (i in 0 until complexKeyPath.size()) {
        keys[i] = complexKeyPath[i].asString.replace("\"".toRegex(), "")
    }
    var currentObject: JsonObject = jsonDb
    for (i in 0 until keys.size - 1) {
        if (!currentObject.has(keys[i])) {
            currentObject.add(keys[i], JsonObject())
        }
        currentObject = currentObject.getAsJsonObject(keys[i])
    }
    currentObject.add(keys[keys.size - 1], newValue)
}

fun deleteKey(targetKey: JsonArray): Boolean {
    var currentObject: JsonObject = jsonDb
    var i = 0
    while (i < targetKey.size() - 1) {
        currentObject = if (currentObject.has(targetKey[i].asString)) {
            currentObject.getAsJsonObject(targetKey[i].asString)
        } else {
            return false
        }
        i++
    }
    return if (currentObject.has(targetKey[i].asString)) {
        currentObject.remove(targetKey[i].asString)
        true
    } else {
        false
    }
}

fun main() {
    val address = "127.0.0.1"
    val port = 23456
    val server = ServerSocket(port, 50, InetAddress.getByName(address))
    println("Server started!")
    reentrantReadWriteLock.readLock().lock()
    try {
        // create the db file if needed
        val file = File(filePath)

        try {
            file.parentFile.mkdirs()
            if (file.exists()) {
                println("File found at: $filePath")
            } else {
                file.createNewFile()
                println("File created successfully at: $filePath")
            }
        } catch (e: Exception) {
            println("Error creating the file: ${e.message}")
        }

        // first two lines here can be commented out to persist the db between server sessions
        jsonDb = JsonObject()
        serialize(jsonDb.toString(), filePath)

        val dbString = deserialize(filePath) as String
        jsonDb = gson.fromJson(dbString, JsonObject::class.java)
        println("DB loaded")
    } catch (e: IOException) {
        e.printStackTrace()
    } catch (e: ClassNotFoundException) {
        e.printStackTrace()
    } finally {
        reentrantReadWriteLock.readLock().unlock()
    }
    val executorService = Executors.newCachedThreadPool()

    while (running) {
        val socket = server.accept()
        executorService.submit(ClientHandler(socket))
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS)
    }
    server.close()
    println("Server closing")
    kotlin.system.exitProcess(0)
}

@Throws(IOException::class)
fun serialize(obj: Any?, fileName: String) {
    val fos = FileOutputStream(fileName)
    val bos = BufferedOutputStream(fos)
    val oos = ObjectOutputStream(bos)
    oos.writeObject(obj)
    oos.close()
}

@Throws(IOException::class, ClassNotFoundException::class)
fun deserialize(fileName: String): Any {
    val fis = FileInputStream(fileName)
    val bis = BufferedInputStream(fis)
    val ois = ObjectInputStream(bis)
    val obj = ois.readObject()
    ois.close()
    return obj
}