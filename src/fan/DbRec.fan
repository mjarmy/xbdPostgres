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
  new make(
    Str id,
    Str[] paths,
    PathRef[] pathRefs,
    Str values,
    Str units,
    Str? spec)
  {
    this.id       = id
    this.paths    = paths
    this.pathRefs = pathRefs
    this.values   = values
    this.units    = units
    this.spec     = spec
  }

  static new fromDict(Dict dict)
  {
    paths := Str[,]
    pathRefs := PathRef[,]
    values := Str:Obj[:]
    units := Str:Obj[:]
    transform(dict, Str[,], paths, pathRefs, values, units)

    return DbRec(
      ((Ref) dict->id)->id,
      paths,
      pathRefs,
      JsonWriter.valToStr(Etc.makeDict(values)),
      JsonWriter.valToStr(Etc.makeDict(units)),
      dict.has("spec") ? ((Ref)dict->spec).id : null)
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
        if (!nvalues.isEmpty)
          values.add(k, Etc.makeDict(nvalues))
        if (!nunits.isEmpty)
          units.add(k, Etc.makeDict(nunits))
      }
      // Ref
      else if (v is Ref)
      {
        if (k != "id")
        {
          r := (Ref) v
          pathRefs.add(PathRef(cp, r.id))
        }
      }
      // Number
      else if (v is Number)
      {
        n := (Number) v

        f := n.toFloat
        if (f.isNaN || f == Float.posInf || f == Float.negInf)
          values.add(k, n)
        else
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

  override Int hash() { id.hash }

  override Bool equals(Obj? that)
  {
    x := that as DbRec
    if (x == null) return false
    return (
      (id == x.id) &&
      (paths == x.paths) &&
      (pathRefs == x.pathRefs) &&
      (values == x.values) &&
      (units == x.units) &&
      (spec == x.spec)
    )
  }

  //////////////////////////////////////////////////////////////
  // Fields
  //////////////////////////////////////////////////////////////

  const Str id
  const Str[] paths
  const PathRef[] pathRefs
  const Str values // hayson
  const Str units  // hayson
  const Str? spec
}

****************************************************************
** PathRef
****************************************************************

const class PathRef
{
  new make(Str path, Str ref)
  {
    this.path = path
    this.ref = ref
  }

  const Str path
  const Str ref

  override Int hash() { path.hash.xor(ref.hash) }

  override Bool equals(Obj? that)
  {
    x := that as PathRef
    if (x == null) return false
    return path == x.path && ref == x.ref
  }
}
