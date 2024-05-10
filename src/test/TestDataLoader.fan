//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack


class TestDataLoader
{
  Void main()
  {
    TestDataLoader().load()
  }

  Void load()
  {
    postgres.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")

    n := 0
    td := TestData()
    td.recs.each |r,id|
    {
      pathRefs := PathRef[,]
      rec := DbRec.fromDict(r, pathRefs)
      postgres.writeRec(rec, pathRefs)
      n++
    }
    echo("loaded $n recs")

    postgres.close()
  }

  private PostgresDb postgres := PostgresDb()
}
