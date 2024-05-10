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
      writeRec(DbRec(r))
      n++
    }
    echo("loaded $n recs")

    postgres.close()
  }

  private Void writeRec(DbRec rec)
  {
    postgres.writeRec(
      rec.id,
      rec.paths,
      rec.pathRefs,
      JsonWriter.valToStr(rec.values),
      JsonWriter.valToStr(rec.units),
      rec.spec == null ? null : rec.spec.id)
  }

  private PostgresDb postgres := PostgresDb()
}
