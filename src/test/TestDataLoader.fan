//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** TestDataLoader insert the TestData into postgres
**
class TestDataLoader
{
  Void main()
  {
    TestDataLoader().load()
  }

  Void load()
  {
    storeHouse.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")

    n := 0
    td := TestData()
//    td.recs.each |r,id|
//    {
//      storeHouse.insertRec(DbRec.fromDict(r))
//      n++
//    }
//    echo("loaded $n recs")
//
    storeHouse.close()
  }

  private Storehouse storeHouse := Storehouse()
}

////
//// Copyright (c) 2024, XetoBase
//// All Rights Reserved
////
//// History:
////   19 Mar 2024  Mike Jarmy  Creation
////
//
//using xeto
//
//**
//** XetoSpecLoader loads the xeto specs into Postgres
//**
//class XetoSpecLoader
//{
//  Void main()
//  {
//    XetoSpecLoader().buildAll()
//  }
//
//  ** Build all the specs from every library
//  Void buildAll()
//  {
//    storeHouse.open(
//      "jdbc:postgresql://localhost/postgres",
//      "xbd",
//      "s3crkEt")
//
//    env := XetoEnv.cur
//    env.registry.list.each |e|
//    {
//      lib := env.lib(e.name)
//      lib.types.each |s| { buildSpec(s) }
//    }
//    echo("loaded $count recs")
//
//    storeHouse.close()
//  }
//
//  ** Build a spec and its inheritance hierarchy
//  private Void buildSpec(Spec spec)
//  {
//    // Ignore synthetic types like "_0" for now
//    if (spec.name.startsWith("_")) return
//
//    //echo("${spec.qname}")
//
//    inherit := [Str:Str][:] /* Set */
//    traverseHierarchy(spec, inherit)
//    storeHouse.insertSpec(spec.qname, inherit.keys)
//    count++
//  }
//
//  ** Recursively traverse the spec's inheritance hierarchy up to the root. If
//  ** the spec has multiple inheritance, multiple paths will be generated.
//  private Void traverseHierarchy(Spec spec, [Str:Str] inherit /* Set */)
//  {
//    // Add to the set of inherited types
//    if (!inherit.containsKey(spec.qname))
//      inherit.add(spec.qname, spec.qname)
//
//    // Mutliple inheritance
//    if (spec.isBaseAnd)
//    {
//      spec.ofs.each |b| { traverseHierarchy(b, inherit) }
//    }
//    // Single inheritance
//    else if (spec.base != null)
//    {
//      traverseHierarchy(spec.base, inherit)
//    }
//  }
//
//  private Storehouse storeHouse := Storehouse()
//  private Int count := 0
//}
