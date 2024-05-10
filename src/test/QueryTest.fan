//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

class QueryTest : Test
{
  override Void setup()
  {
    postgres.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")
  }

  override Void teardown()
  {
    postgres.close
  }

  Void testQuery()
  {
    f :=  Filter("ahu")
    expected := testData.filter(f)
    echo(expected.keys)

    q := Query.fromFilter(f)
    verifyEq(q, Query(
      """select * from rec
         where
           (rec.paths @> ?::text[]);""",
      Str["{\"ahu\"}"]))

    found := postgres.query(q)
    verifyQuery(found, expected)
  }

  private Void verifyQuery(DbRec[] found, Ref:Dict expected)
  {
    verifyEq(found.size, expected.size)
    found.each |f|
    {
      Ref id := Ref.fromStr(f.id)
      e := DbRec.fromDict(expected.get(id), PathRef[,])

      verifyEq(f.id, e.id)
      verifyEq(f.paths, e.paths)
      verifyTrue(Etc.dictEq(
        JsonReader(f.values.in).readVal,
        JsonReader(e.values.in).readVal))
      verifyTrue(Etc.dictEq(
        JsonReader(f.refs.in).readVal,
        JsonReader(e.refs.in).readVal))
      verifyTrue(Etc.dictEq(
        JsonReader(f.units.in).readVal,
        JsonReader(e.units.in).readVal))
      verifyEq(f.spec, e.spec)
    }
  }

  private TestData testData := TestData()
  private PostgresDb postgres := PostgresDb()
}
