//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack


class JsonLoader
{
  Void main()
  {
    JsonLoader().load()
  }

  Void load()
  {
    postgres.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")

    // alpha
    Grid alpha := JsonReader(File(`test_data/alpha.json`).in).readVal
    alpha.each |row, i|
    {
      DbRec rec := DbRec(row)
      writeRec(rec)
    }

    // niagara
    f := File(`test_data/jason.txt`)
    f.eachLine |line|
    {
      DbRec rec := DbRec(JsonReader(line.in).readVal)
      writeRec(rec)
    }

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

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private PostgresDb postgres := PostgresDb()
}
