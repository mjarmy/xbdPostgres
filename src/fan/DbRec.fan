//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** DbRec is a record in the postgres 'rec' table
**
const class DbRec
{
  new make(
    Ref id,
    Str[] paths,
    Dict values,
    Dict refs,
    Dict units,
    Ref? spec)
  {
    this.id     = id
    this.paths  = paths
    this.refs   = refs
    this.values = values
    this.units  = units
    this.spec   = spec
  }

  static new fromDict(Dict dict)
  {
    paths := Str[,]
    values := Str:Obj[:]
    refs := Str:Obj[:]
    units := Str:Obj[:]
    traverseDict(dict, Str[,], paths, values, refs, units)

    return DbRec(
      dict.id,
      paths,
      Etc.makeDict(values),
      Etc.makeDict(refs),
      Etc.makeDict(units),
      dict.get("spec", null))
  }

  private static Void traverseDict(
      Dict d, Str[] curPath, Str[] paths,
      Str:Obj values, Str:Obj refs, Str:Obj units)
  {
    d.each |v,k|
    {
      curPath.add(k)
      cp := curPath.join(".")
      paths.add(cp)

      // traverseDict nested dict
      if (v is Dict)
      {
        Str:Obj nvalues := Str:Obj[:]
        Str:Obj nrefs := Str:Obj[:]
        Str:Obj nunits := Str:Obj[:]
        traverseDict(v, curPath, paths, nvalues, nrefs, nunits)
        if (!nvalues.isEmpty)
          values.add(k, Etc.makeDict(nvalues))
        if (!nrefs.isEmpty)
          refs.add(k, Etc.makeDict(nrefs))
        if (!nunits.isEmpty)
          units.add(k, Etc.makeDict(nunits))
      }
      // Ref
      else if (v is Ref)
      {
        if (k != "id")
        {
          refs.add(k, (Ref) v)
        }
      }
      // Number
      else if (v is Number)
      {
        n := (Number) v

        // Strip units
        if (n.unit != null)
        {
          units.add(k, n.unit.toStr)
          n = n.isInt ?
            Number.makeInt(n.toInt) :
            Number(n.toFloat)
        }
        values.add(k, n)
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
      Etc.dictEq(values, x.values) &&
      Etc.dictEq(refs, x.refs) &&
      Etc.dictEq(units, x.units) &&
      (spec == x.spec)
    )
  }

  override Str toStr() { "DbRec($id)" }

  // Return a Map of the path to each Ref.
  Str:Ref pathRefs()
  {
    pathRefs := Str:Ref[:]
    traverseRefs(refs, Str[,], pathRefs)
    return pathRefs
  }

  private static Void traverseRefs(
      Dict refs, Str[] curPath, Str:Ref pathRefs)
  {
    refs.each |v,k|
    {
      curPath.add(k)
      cp := curPath.join(".")

      if (v is Dict)
      {
        traverseRefs(v, curPath, pathRefs)
      }
      else if (v is Ref)
      {
        pathRefs.add(cp, v)
      }

      curPath.removeAt(-1)
    }
  }

  //-----------------------------------------------
  // Fields
  //-----------------------------------------------

  const Ref id

  // Path to every value
  const Str[] paths

  // Contains everything but Markers and Refs.
  // The Number values have been stripped of units.
  const Dict values

  const Dict refs
  const Dict units
  const Ref? spec
}
