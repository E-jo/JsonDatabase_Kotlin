package jsondatabase.client

import com.google.gson.Gson
import com.google.gson.JsonObject
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import java.io.BufferedReader
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.FileReader
import java.net.InetAddress
import java.net.Socket
import java.nio.file.Path
import java.nio.file.Paths

val filePath = "./src/jsondatabase/client/data/"

fun main(args: Array<String>) {
    val parser = ArgParser("MainKt")

    val requestType by parser.option(
        ArgType.String,
        shortName = "t",
        description = "Request type"
    )

    val requestKey by parser.option(
        ArgType.String,
        shortName = "k",
        description = "Request key"
    )

    val requestValue by parser.option(
        ArgType.String,
        shortName = "v",
        description = "Request value"
    )

    val requestFile by parser.option(
        ArgType.String,
        shortName = "in",
        description = "Request from file"
    )

    parser.parse(args)

    val address = "127.0.0.1"
    val port = 23456
    val socket = Socket(InetAddress.getByName(address), port)
    println("Client started!")

    val input = DataInputStream(socket.getInputStream())
    val output = DataOutputStream(socket.getOutputStream())

    var request = JsonObject()

    if (!requestFile.isNullOrEmpty()) {
        val inputFilePath: Path = Paths.get(filePath + requestFile)
        val inputFile = inputFilePath.toFile()
        val `in` = BufferedReader(FileReader(inputFile))
        val fileInputStr = `in`.readLine()
        `in`.close()
        val gson = Gson()
        request = gson.fromJson(fileInputStr, JsonObject::class.java)
    } else {
        request.addProperty("type", requestType)
        if (requestType?.lowercase() != "exit")
            request.addProperty("key", requestKey)
        if (requestType?.lowercase() == "set")
            request.addProperty("value", requestValue)
    }

    val gson = Gson()
    val jsonRequest = gson.toJson(request)

    output.writeUTF(jsonRequest)
    println("Sent: $jsonRequest")

    val serverResponse = input.readUTF()
    println("Received: $serverResponse")

    socket.close()
}


