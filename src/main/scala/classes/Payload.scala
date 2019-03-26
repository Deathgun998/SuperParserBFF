package classes

case class Payload (
                     push_id: Int,
                     size: Int,
                     distinct_size: Int,
                     ref: String,
                     head: String,
                     before: String,
                     commits: Seq[Commit]
                   )