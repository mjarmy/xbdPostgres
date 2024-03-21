//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   19 Mar 2024  Mike Jarmy  Creation
//

using xeto

class SpecBuilder
{
  Void main()
  {
    SpecBuilder().buildAll()
  }

//////////////////////////////////////////////////////////////////////////
// Methods
//////////////////////////////////////////////////////////////////////////

  // Build all the specs from every library
  Void buildAll()
  {
    ds.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")

    env := XetoEnv.cur
    env.registry.list.each |e|
    {
      lib := env.lib(e.name)
      lib.types.each |s| { buildSpec(s) }
    }

    ds.close()
  }

  // Build a spec and its inheritance hierarchy
  internal Void buildSpec(Spec spec)
  {
    // Ignore synthetic types like "_0" for now
    if (spec.name.startsWith("_")) return

    ds.writeSpec(spec.qname)
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  internal DbSpec ds := DbSpec()
}
