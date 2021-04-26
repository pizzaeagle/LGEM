package mm.graph.embeddings

import com.typesafe.config.Config

case class Configuration(
  embeddingType: String,
  relationsInputDir: String,
  nodeFeaturesInputDir: String,
  outputDir: String
)

object Configuration {
  def apply(config: Config): Configuration =
    new Configuration(
      embeddingType = config.getString("embeddingType"),
      relationsInputDir = config.getString("relationsInputDir"),
      nodeFeaturesInputDir = config.getString("nodeFeaturesInputDir"),
      outputDir = config.getString("outputDir")
    )
}
