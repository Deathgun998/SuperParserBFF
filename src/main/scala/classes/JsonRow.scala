package classes

import java.sql.Timestamp

case class JsonRow (
                     id: String,
                     eventType: String,
                     actor: Actor,
                     repo: Repo,
                     payload: Payload,
                     isPublic: Boolean,
                     created_at: String
)
