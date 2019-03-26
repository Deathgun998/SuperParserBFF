package classes

case class JsonRow (
                           id: String,
                           EventType: String,
                           actor: Actor,
                           repo: Repo,
                           payload: Payload,
                           public: Boolean,
                           created_at: String
)
