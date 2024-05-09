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
    Str:Obj values := Str:Obj[:]
    transform(hayson, Str[,], paths, values)

    this.paths = paths
    this.values = Etc.makeDict(values)
  }

  private static Void transform(
      Dict d, Str[] curPath, Str[] paths, Str:Obj values)
  {
    d.each |v,k|
    {
      curPath.add(k)
      paths.add(curPath.join("."))

      // transform nested dict
      if (v is Dict)
      {
        Str:Obj nested := Str:Obj[:]
        transform(v, curPath, paths, nested)
        values.add(k, Etc.makeDict(nested))
      }
      else if (v is Ref)
      {

      }
      // remove markers
      else if (!(v is Marker))
      {
        values.add(k, v)
      }

      curPath.removeAt(-1)
    }
  }

  override Str toStr() { "DbRec('$id', $paths, ${JsonWriter.valToStr(values)})" }

  //////////////////////////////////////////////////////////////
  // Fields
  //////////////////////////////////////////////////////////////

  const Str id
  const Str[] paths
  const Dict values
}

****************************************************************
** DbRec
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

  override Bool equals(Obj? that)
  {
    x := that as PathRef
    if (x == null) return false
    return path == x.path && ref.id == x.ref.id
  }

  override Str toStr() { "PathRef($path, $ref)" }
}
