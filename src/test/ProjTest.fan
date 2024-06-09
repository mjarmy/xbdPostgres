//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using sql

**
** ProjTest
**
class ProjTest : Test
{
  Void testMultipleProjects()
  {
    pool := HavenPool
    {
      it.uri      = "jdbc:postgresql://localhost/postgres"
      it.username = "xbd"
      it.password = "s3crkEt"
    }

    // set up multiple haven projects
    Haven[] havens := Str["proj1", "proj2"].map |Str proj->Haven|
    {
      echo("nuke")
      pool.execute(|SqlConn conn| { TestDataLoader.nuke(conn, proj) })

      echo("init")
      haven := Haven { it.projName = proj; it.pool = pool }
      haven.init
      return haven
    }

    // load test data
    prefix := "../xetobase/xb-play/data/test/haven/"
    alphaFile := Env.cur.findFile(Uri(prefix + "alpha.json"))
    Grid alpha := JsonReader(alphaFile.in).readVal

    havens.each |haven|
    {
      echo("Loading ${haven.projName}")
      alpha.each |dict|
      {
        id := dict->id
        haven.create(Etc.dictRemove(dict, "id"), id)
      }
    }

    // done
    echo("Done")
    pool.close
  }
}
