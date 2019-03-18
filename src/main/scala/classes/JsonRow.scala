package classes

case class JsonRow (
                           id: String,
                           `type`: String,
                           actor: Actor,
                           repo: Repo,
                           payload: Payload,
                           public: Boolean,
                           created_at: String
)
