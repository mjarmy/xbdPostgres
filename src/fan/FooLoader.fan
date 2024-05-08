//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using util

class FooLoader
{
  Void main()
  {
    FooLoader().loadFooData()
  }

//////////////////////////////////////////////////////////////////////////
// Methods
//////////////////////////////////////////////////////////////////////////

  Void loadFooData()
  {
    postgres := PostgresDb()
    postgres.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")

    //seed := DateTime.nowTicks()
    seed := 768413414708000000
    echo("seed: $seed")
    Random rnd := Random.makeSeeded(seed)

    //(0..<100_000).each |n|
    (0..<10_000).each |n|
    //(0..<10).each |n|
    {
      if ((n % 1000) == 0) echo(".")

      id := "$n"
      Str:Obj? map := [:]
      map.add("id", id)

      generateStruct(rnd, map)
      rec := Etc.makeDict(map)
      paths := findPaths(rec, [,], [,])
      hayson := JsonWriter.valToStr(rec)

      //echo("----------------------------------------")
      //echo(paths)
      //echo(hayson)

      postgres.writeFoo(id, paths, hayson)
    }

    echo("")
    echo("done")

    postgres.close()
  }

  private Void generateStruct(Random rnd, Str:Obj? map)
  {
    Str[] tags := generateTags(rnd)
    tags.each |t|
    {
      if (rnd.next(0..<10) <= 1)
      {
        Str:Obj? s := [:]
        generateStruct(rnd, s)
        map.add(t, Etc.makeDict(s))
      } else
      {
        map.add(t, rnd.next(0..4))
      }
    }
  }

  private Str[] generateTags(Random rnd)
  {
    Str:Str tags := [:]

    numTags := rnd.next(1..allowedTags.size)
    while (tags.size < numTags)
    {
      t := allowedTags[rnd.next(0..<allowedTags.size)]
      if (!tags.containsKey(t))
        tags.add(t, t)
    }

    return tags.keys
  }

  private Str[] findPaths(Dict d, Str[] curPath, Str[] paths)
  {
    d.each |v,k|
    {
      curPath.add(k)
      paths.add(curPath.join("."))

      if (v is Dict)
      {
        findPaths(v, curPath, paths)
      }

      curPath.removeAt(-1)
    }
    return paths
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  //private static const Str[] allowedTags := ["a", "b", "c", "d", "e", "f", "g", "h"]
  private static const Str[] allowedTags := ["a", "b", "c", "d"]

  private PostgresDb postgres := PostgresDb()
}
