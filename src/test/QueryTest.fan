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
    expected := testData.filter(f).keys.sort
    echo(expected)

    q :=  Query.fromFilter(f)
    verifyEq(q, Query(
      """select * from rec
         where
           (rec.paths @> ?::text[]);""",
      Str["{\"ahu\"}"]))

    echo(postgres.query(q))
  }

  private TestData testData := TestData()
  private PostgresDb postgres := PostgresDb()
}
