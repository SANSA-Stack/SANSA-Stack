resolvers ++= Seq(
  Resolver.url("NetBeans", url("http://bits.netbeans.org/nexus/content/groups/netbeans/")).withAllowInsecureProtocol(true),
  Resolver.url("gephi-thirdparty", url("https://raw.github.com/gephi/gephi/mvn-thirdparty-repo/")),
  Resolver.sonatypeRepo("snapshots")
)

import scala.concurrent.duration.DurationInt
import lmcoursier.definitions.CachePolicy

csrConfiguration := csrConfiguration.value
  .withTtl(1.minute)
  .withCachePolicies(Vector(CachePolicy.LocalOnly))