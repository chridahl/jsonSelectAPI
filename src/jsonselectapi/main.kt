package jsonselectapi

import com.google.common.reflect.TypeToken
import com.google.gson.Gson
import org.apache.log4j.BasicConfigurator
import org.apache.metamodel.DataContext
import org.apache.metamodel.DataContextFactory
import org.apache.metamodel.csv.CsvDataContext
import org.apache.metamodel.json.JsonDataContext
import spark.Spark.get
import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.nio.channels.Channels
import java.util.*

fun main(args: Array<String>) {
    BasicConfigurator.configure()
    setupRoutes()
}

data class DataSourceRequest(val alias: String, val url: String, val dataType: String)

fun executeQueryOnContext(dataContext: DataContext, query: String): String {

    val result = StringBuffer()
    val dataSet = dataContext.executeQuery(query)
    val textualRows = ArrayList<ArrayList<String>>() // ugh..

    val gson = Gson()
    while (dataSet.next()) {

        val tmpRow = dataSet.row.values.mapTo(ArrayList<String>()) { it.toString() }
        textualRows.add(tmpRow)
    }

    result.append(gson.toJson(textualRows))

    return result.toString()
}


fun executeSelect(sources: ArrayList<DataSourceRequest>, query: String): String {

    val dataContexts = ArrayList<DataContext>()

    for ((alias, url, dataType) in sources) {

        val filename = String.format("%s.%s", alias, dataType)
        val file = File(filename)

        if (!file.exists()) {
            val website = URL(url)
            val rbc = Channels.newChannel(website.openStream())
            val fos = FileOutputStream(filename)
            fos.channel.transferFrom(rbc, 0, java.lang.Long.MAX_VALUE)
        }

        when (dataType) {
            "json" -> dataContexts.add(JsonDataContext(file))
            "csv" -> dataContexts.add(CsvDataContext(file))
        }
    }

    val dataContext = DataContextFactory.createCompositeDataContext(dataContexts)

    return executeQueryOnContext(dataContext, query)
}


fun setupRoutes() {
    get("/select", fun(request, response): Any? {

        val payload = request.queryParams("selectRequest")
        val query = request.queryParams("query")

        val listType = object : TypeToken<ArrayList<DataSourceRequest>>(){}.type

        val gson = Gson()
        val dataSources = gson.fromJson<ArrayList<DataSourceRequest>>(payload, listType)

        return executeSelect(dataSources, query)
    })
}

