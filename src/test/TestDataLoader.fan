////
//// Copyright (c) 2024, XetoBase
//// All Rights Reserved
////
//// History:
////   6 May 2024  Mike Jarmy  Creation
////
//
//using haystack
//using xeto
//
//**
//** TestDataLoader insert the TestData into postgres
//**
//class TestDataLoader
//{
//  Void main()
//  {
//    TestDataLoader().load()
//  }
//
//  Void load()
//  {
//    haven = Haven.open(
//      "jdbc:postgresql://localhost/postgres",
//      "xbd",
//      "s3crkEt")
//
//    haven.testConn.sql("delete from spec").execute
//    haven.testConn.sql("delete from ref_tag").execute
//    haven.testConn.sql("delete from path_ref").execute
//    haven.testConn.sql("delete from rec").execute
//
//    td := TestData()
//    loadSpecs(td)
//    loadRecs(td)
//
//    haven.close()
//  }
//
//  ** load all the specs
//  internal Void loadSpecs(TestData td)
//  {
//    n := 0
//    td.xetoNs.libs.each |lib|
//    {
//      lib.specs.each |s| {
//        loadSpec(s)
//        n++
//      }
//    }
//    echo("loaded $n specs")
//  }
//
//  ** Load a xeto spec and its inheritance hierarchy
//  private Void loadSpec(Spec spec)
//  {
//    // Ignore synthetic types like "_0" for now
//    if (spec.name.startsWith("_")) return
//
//    //echo("${spec.qname}")
//
//    inherit := [Str:Str][:] /* Set */
//    traverseHierarchy(spec, inherit)
//    haven.createSpec(spec.qname, inherit.keys)
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
//    if (spec.isAnd)
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
//  ** load the recs from the testdata
//  private Void loadRecs(TestData td)
//  {
//    n := 0
//    td.recs.each |dict|
//    {
//      id := dict->id
//      haven.create(Etc.dictRemove(dict, "id"), id)
//      n++
//    }
//    echo("loaded $n recs")
//  }
//
//  private Haven? haven
//}
