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
    JsonLoader().loadTestData()
  }

//////////////////////////////////////////////////////////////////////////
// Methods
//////////////////////////////////////////////////////////////////////////

  Void loadTestData()
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

      arrows := Arrow[,]
      findArrows(rec, Str[,], arrows)
      postgres.writeRec(id, JsonWriter.valToStr(rec), arrows)
    }

    postgres.close()
  }

  private Void findArrows(Dict rec, Str[] path, Arrow[] arrows)
  {
    rec.each |val, name|
    {
      if ((name != "id") && (val is Ref))
      {
        path.add(name)
        arrows.add(Arrow(path.join("."), ((Ref) val).id))
        path.removeAt(-1)
      }
      else if (val is Dict)
      {
        path.add(name)
        findArrows((Dict) val, path, arrows)
        path.removeAt(-1)
      }
    }
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  internal PostgresDb postgres := PostgresDb()
}

//////////////////////////////////////////////////////////////////////////
// Arrow
//////////////////////////////////////////////////////////////////////////

const class Arrow
{
  new make(Str toPath, Str toId)
  {
    this.toPath = toPath
    this.toId = toId
  }

  const Str toPath
  const Str toId

  override Str toStr() { "Arrow($toPath, $toId)" }
}
