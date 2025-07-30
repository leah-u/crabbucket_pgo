import crabbucket/pgo.{HasRemainingTokens, remaining_tokens_for_key} as crabbucket
import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor
import gleam/result
import gleeunit
import gleeunit/should
import pog

fn get_db() {
  let name = process.new_name("db")
  let assert Ok(actor.Started(_pid, db)) =
    pog.default_config(name)
    |> pog.host("127.0.0.1")
    |> pog.user("postgres")
    |> pog.password(Some("postgres"))
    |> pog.database("crabbucket_test")
    |> pog.pool_size(15)
    |> pog.start

  let assert Ok(_) =
    pog.query(crabbucket.schema_migration_sql) |> pog.execute(db)
  let assert Ok(_) =
    pog.query(crabbucket.table_migration_sql) |> pog.execute(db)
  let assert Ok(_) =
    pog.query(crabbucket.index_migration_sql) |> pog.execute(db)

  db
}

pub fn main() {
  gleeunit.main()
}

pub fn insert_test() {
  let db = get_db()
  let window_duration_ms = 60 * 1000
  let default_remaining_tokens = 2
  let key = "test entry"

  let HasRemainingTokens(remaining1, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining1
  |> should.equal(default_remaining_tokens - 1)

  let HasRemainingTokens(remaining2, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining2
  |> should.equal(default_remaining_tokens - 2)

  remaining_tokens_for_key(
    db,
    key,
    window_duration_ms,
    default_remaining_tokens,
  )
  |> should.be_error()

  Nil
}

pub fn expiration_test() {
  let db = get_db()
  let window_duration_ms = 1000
  let default_remaining_tokens = 1
  let key = "test entry 2"

  let HasRemainingTokens(remaining1, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining1
  |> should.equal(default_remaining_tokens - 1)

  remaining_tokens_for_key(
    db,
    key,
    window_duration_ms,
    default_remaining_tokens,
  )
  |> should.be_error()

  process.sleep(1000)

  let HasRemainingTokens(remaining1, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining1
  |> should.equal(default_remaining_tokens - 1)
}

pub fn atomic_stress_test() {
  let db = get_db()
  let window_duration_ms = 60 * 1000
  let default_remaining_tokens = 100
  let key = "test entry 3"

  let results =
    list.range(1, 500)
    |> list.map(fn(_) {
      let subject = process.new_subject()
      process.spawn(fn() {
        process.send(
          subject,
          remaining_tokens_for_key(
            db,
            key,
            window_duration_ms,
            default_remaining_tokens,
          ),
        )
      })
      subject
    })
    |> list.map(process.receive_forever)

  results
  |> list.count(fn(res) { result.is_ok(res) })
  |> should.equal(100)

  results
  |> list.count(fn(res) { result.is_error(res) })
  |> should.equal(400)
}

pub fn cleaner_test() {
  let db = get_db()
  let window_duration_ms = 1000
  let default_remaining_tokens = 100
  let key = "test entry 4"

  let HasRemainingTokens(remaining, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining
  |> should.equal(default_remaining_tokens - 1)

  let _ = crabbucket.create_and_start_cleaner(db, 1000)

  process.sleep(2000)

  let assert Ok(response) =
    pog.query("SELECT NULL FROM crabbucket.token_buckets WHERE id = $1")
    |> pog.parameter(pog.text(key))
    |> pog.execute(db)

  response.rows
  |> should.equal([])
}
