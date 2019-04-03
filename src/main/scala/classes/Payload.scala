package classes

case class Payload (
                     push_id: BigInt,
                     size: BigInt,
                     distinct_size: BigInt,
                     ref: String,
                     head: String,
                     before: String,
                     commits: Seq[Commit]
                   )