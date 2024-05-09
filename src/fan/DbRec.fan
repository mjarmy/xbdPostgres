//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

****************************************************************
** DbRec
****************************************************************

const class DbRec
{
  new make(Dict hayson)
  {
    this.id = ((Ref) hayson->id)->id

    paths := Str[,]
    pathRefs := PathRef[,]
    Str:Obj values := Str:Obj[:]
    Str:Obj units := Str:Obj[:]
    transform(hayson, Str[,], paths, pathRefs, values, units)

    this.paths = paths
    this.pathRefs = pathRefs
    this.values = Etc.makeDict(values)
    this.units = Etc.makeDict(units)
  }

  private static Void transform(
      Dict d, Str[] curPath, Str[] paths, PathRef[] pathRefs,
      Str:Obj values, Str:Obj units)
  {
    d.each |v,k|
    {
      curPath.add(k)
      cp := curPath.join(".")
      paths.add(cp)

      // transform nested dict
      if (v is Dict)
      {
        Str:Obj nvalues := Str:Obj[:]
        Str:Obj nunits := Str:Obj[:]
        transform(v, curPath, paths, pathRefs, nvalues, nunits)
        values.add(k, Etc.makeDict(nvalues))
        if (!nunits.isEmpty)
          units.add(k, Etc.makeDict(nunits))
      }
      // Ref
      else if (v is Ref)
      {
        if (k != "id")
          pathRefs.add(PathRef(cp, v))
      }
      // Number
      else if (v is Number)
      {
        n := (Number) v
        values.add(k, n.toFloat)
        if (n.unit != null)
          units.add(k, n.unit.toStr)
      }
      // remove markers
      else if (!(v is Marker))
      {
        values.add(k, v)
      }

      curPath.removeAt(-1)
    }
  }

  //////////////////////////////////////////////////////////////
  // Fields
  //////////////////////////////////////////////////////////////

  const Str id
  const Str[] paths
  const PathRef[] pathRefs
  const Dict values
  const Dict units
}

****************************************************************
** PathRef
****************************************************************

const class PathRef
{
  new make(Str path, Ref ref)
  {
    this.path = path
    this.ref = ref
  }

  const Str path
  const Ref ref

  override Int hash() { path.hash.xor(ref.id.hash) }

  override Bool equals(Obj? that)
  {
    x := that as PathRef
    if (x == null) return false
    return path == x.path && ref.id == x.ref.id
  }

  override Str toStr() { "PathRef($path, $ref)" }
}
