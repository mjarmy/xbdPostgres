//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using sql
using xeto

**
** TestDataLoader insert the TestData into postgres
**
class TestDataLoader
{
  Void main()
  {
    pool := HavenPool
    {
      it.uri      = "jdbc:postgresql://localhost/postgres"
      it.username = "xbd"
      it.password = "s3crkEt"
    }

    // nuke old records if they exist
    pool.execute(|SqlConn conn| { nuke(conn, "test_proj") })

    // create Haven
    haven := Haven("test_proj", pool)
    haven.init

    // load the data
    td := TestData()
    loadSpecs(haven, td)
    loadRecs(haven, td)

    // done
    pool.close
  }

  internal static Void nuke(SqlConn conn, Str projName)
  {
    if (Haven.schemaExists(conn, projName))
    {
      conn.sql("set search_path to $projName").execute

      conn.sql("delete from spec").execute
      conn.sql("delete from ref_tag").execute
      conn.sql("delete from path_ref").execute
      conn.sql("delete from rec").execute

      conn.commit
    }
  }

  ** load all the specs
  internal static Void loadSpecs(Haven haven, TestData td)
  {
    n := 0
    td.xetoNs.libs.each |lib|
    {
      lib.specs.each |s| {
        loadSpec(haven, s)
        n++
      }
    }
    echo("loaded $n specs")
  }

  ** Load a xeto spec and its inheritance hierarchy
  private static Void loadSpec(Haven haven, Spec spec)
  {
    // Ignore synthetic types like "_0" for now
    if (spec.name.startsWith("_")) return

    //echo("${spec.qname}")

    inherit := [Str:Str][:] /* Set */
    traverseHierarchy(spec, inherit)
    haven.createSpec(spec.qname, inherit.keys)
  }

  ** Recursively traverse the spec's inheritance hierarchy up to the root. If
  ** the spec has multiple inheritance, multiple paths will be generated.
  private static Void traverseHierarchy(Spec spec, [Str:Str] inherit /* Set */)
  {
    // Add to the set of inherited types
    if (!inherit.containsKey(spec.qname))
      inherit.add(spec.qname, spec.qname)

    // Mutliple inheritance
    if (spec.isAnd)
    {
      spec.ofs.each |b| { traverseHierarchy(b, inherit) }
    }
    // Single inheritance
    else if (spec.base != null)
    {
      traverseHierarchy(spec.base, inherit)
    }
  }

  ** load the recs from the testdata
  internal static Void loadRecs(Haven haven, TestData td)
  {
    n := 0
    td.recs.each |dict|
    {
      id := dict->id
      haven.create(Etc.dictRemove(dict, "id"), id)
      n++
    }
    echo("loaded $n recs")
  }
}
