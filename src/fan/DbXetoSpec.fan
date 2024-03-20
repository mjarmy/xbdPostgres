//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   19 Mar 2024  Mike Jarmy  Creation
//
using xeto

**
** Utility for populating postgres xeto spec tables
**
class DbXetoSpec
{
  Void main()
  {
    dx := DbXetoSpec()
    //dx.openDb
    dx.populateAll
    //dx.closeDb
  }

  // Populate all the specs from every library
  Void populateAll()
  {
    env := XetoEnv.cur
    env.registry.list.each |e|
    {
      lib := env.lib(e.name)
      lib.types.each |s| { populateSpec(s) }
    }
  }

  // Populate a spec and its inheritance hierarchy
  internal Void populateSpec(Spec spec)
  {
    // Ignore synthetic types like "_0" for now
    if (spec.name.startsWith("_")) return

    debug("${spec.qname}")
    //writeDbSpec(spec.qname)
    //traverseHierarchy(spec, [,])
  }

  //// Traverse the specs inheritance hierarchy 'backwards' up to the root.
  //// If the spec has multiple inheritance, multiple paths will be generated.
  //internal Void traverseHierarchy(Spec spec, Str[] path)
  //{
  //  path.add(spec.qname)

  //  // Mutliple inheritance
  //  if (spec.isBaseAnd)
  //  {
  //    spec.ofs.each |b| { traverseHierarchy(b, path) }
  //  }
  //  // Root
  //  else if (spec.base == null)
  //  {
  //    // generate a path
  //    populateHierarchy(path)
  //  }
  //  // Single inheritance
  //  else
  //  {
  //    traverseHierarchy(spec.base, path)
  //  }

  //  path.removeAt(-1)
  //}

  //internal Void populateHierarchy(Str[] path)
  //{
  //  Str[] forwards := [,]
  //  path.eachr |s| { forwards.add(s) }
  //  debug("    $forwards")
  //}

  internal Void debug(Str msg)
  {
    echo(msg)
  }

//////////////////////////////////////////////////////////////////////////
// Native
//////////////////////////////////////////////////////////////////////////

  //native Void openDb()
  //native Void closeDb()
  //native Int writeDbSpec(Str specName)
}
