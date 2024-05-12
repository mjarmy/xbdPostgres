//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   10 May 2024  Mike Jarmy  Creation
//

using haystack
using sql

**
** TestSqlPod is a temporary test to help fix sql.pod when necessary
**
class TestSqlPod : Test
{
  Void testSqlPod()
  {
    conn := SqlConn.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")

    delete := conn.sql("delete from foo").prepare
    delete.execute()

    Dict vals := Etc.makeDict1("foo", "bar")
    Str haysonVals := JsonWriter.valToStr(vals)
    insert := conn.sql(
      "insert into foo (id, paths, values_) values (@id, @paths, @values::jsonb)").prepare
    insert.execute([
      "id":"abc",
      "paths":["x", "y", "z"],
      "values": haysonVals
    ])

    select := conn.sql("select * from foo where id = @id").prepare
    rows := select.query(["id":"abc"])
    verifyEq(rows.size, 1)
    f := rows[0]
    verifyEq(f->id, "abc")
    verifyEq(f->paths, ["x", "y", "z"])
    verifyTrue(Etc.dictEq(
      JsonReader(((Str)f->values_).in).readVal,
      vals))

    filter := conn.sql(
      "select * from foo where (foo.paths @> @path::text[])").prepare
    rows = filter.query(["path":"{\"x\"}"])
    verifyEq(rows.size, 1)
    f = rows[0]
    verifyEq(f->id, "abc")
    verifyEq(f->paths, ["x", "y", "z"])
    verifyTrue(Etc.dictEq(
      JsonReader(((Str)f->values_).in).readVal,
      vals))

    filter = conn.sql(
      "select * from foo where (foo.values_ @> @values::jsonb)").prepare
    rows = filter.query(["values":haysonVals])
    verifyEq(rows.size, 1)
    f = rows[0]
    verifyEq(f->id, "abc")
    verifyEq(f->paths, ["x", "y", "z"])
    verifyTrue(Etc.dictEq(
      JsonReader(((Str)f->values_).in).readVal,
      vals))

    conn.close
  }
}
