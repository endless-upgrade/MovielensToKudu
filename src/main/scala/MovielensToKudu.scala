import it.reply.data.devops.KuduStorage
import it.reply.data.pasquali.Storage
import org.rogach.scallop.ScallopConf

object MovielensToKudu {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val table = opt[String]()
    val addr = opt[String]()
    val port = opt[String]()
    dependsOnAll(addr, List(port))
    verify()
  }

  def main(args: Array[String]): Unit = {

    val arr = new Args(args)

    var addr = "35.229.41.198"
    var port = "7051"
    var table = ""

    if(arr.addr.isSupplied){
      addr = arr.addr()
      port = arr.port()
    }

    if(arr.table.isSupplied){
      table = arr.table()
    }

    val storage = Storage()
      .init("local", "kudu", false)
      .initKudu(addr, port, "impala::")

    table match {
      case "" => {
        KuduStorage(storage).storeRatingsToKudu("data/ratings.csv")
        KuduStorage(storage).storeMoviesToKudu("data/links.csv")
        KuduStorage(storage).storeTagsToKudu("data/movies.csv")
        KuduStorage(storage).storeLinksToKudu("data/tags.csv")
      }

      case "ratings" => {
        KuduStorage(storage).storeRatingsToKudu("data/ratings.csv")
      }

      case "links" => {
        KuduStorage(storage).storeLinksToKudu("data/tags.csv")
      }

      case "movies" => {
        KuduStorage(storage).storeMoviesToKudu("data/links.csv")
      }

      case "tags" => {
        KuduStorage(storage).storeTagsToKudu("data/movies.csv")
      }

      case _ => println("Only ratings, tags, links and movies allowed")
    }



  }

}
