import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.{After, Before, Test}

import java.net.URI

class HdfsClientTest {
  var fs: FileSystem = _;
  var conf: Configuration = _;

  @Before
  def connect2HDFS(): Unit = {
    conf = new Configuration()
    val user = "root"
    val uri = new URI("hdfs://hadoop1:8020")
    //      System.setProperty("HADOOP_USER_NAME","root")
    //    conf.set("fs.defaultFS", "hdfs://hadoop1:8020")
    fs = FileSystem.get(uri, conf, user)
    println("before")
  }

  @Test
  def mkdir(): Unit = {
    // 首先判断文件夹是否存在
    if (!fs.exists(new Path("/test"))) {
      fs.mkdirs(new Path("/test"))
      println("main")
    }
  }

  @After
  def close(): Unit = {
    if (fs != null) {
      println("close 关闭了")
      fs.close()
    }
  }
}
