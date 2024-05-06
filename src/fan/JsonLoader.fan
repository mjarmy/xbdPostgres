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
    postgres := PostgresDb()
    postgres.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")

    f := File(`test_data/jason.txt`)
    f.eachLine |line|
    {
      Dict rec := JsonReader(line.in).readVal
      Str id := ((Ref)rec->id).id
      echo(id)
      postgres.writeRec(id, JsonWriter.valToStr(rec))
    }

    postgres.close()
  }
}
