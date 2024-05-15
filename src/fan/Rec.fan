//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Rec is a record in the postgres 'rec' table.
**
internal const class Rec
{
  **
  ** Only used for testing
  **
  internal new make(
    Ref id,
    Str[] paths,
    Dict values,
    Str:Ref pathRefs,
    Ref? spec)
  {
    this.id       = id
    this.paths    = paths
    this.values   = values
    this.pathRefs = pathRefs
    this.spec     = spec
  }

  **
  ** Create a Rec by transforming a Dict
  **
  internal static new fromDict(Dict dict)
  {
    paths := Str[,]
    values := Str:Obj[:]
    pathRefs := Str:Ref[:]

    traverseDict(dict, Str[,], paths, values, pathRefs)

    return Rec(
      dict.id,
      paths,
      Etc.makeDict(values),
      pathRefs,
      dict.get("spec", null))
  }

  private static Void traverseDict(
      Dict d, Str[] curPath, Str[] paths,
      Str:Obj values, Str:Ref pathRefs)
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
        traverseDict(v, curPath, paths, nvalues, pathRefs)
        if (!nvalues.isEmpty)
          values.add(k, Etc.makeDict(nvalues))
      }
      // Ref
      else if (v is Ref)
      {
        if (k != "id")
        {
          pathRefs.add(cp, v)
        }
        values.add(k, v)
      }
      // Number
      else if (v is Number)
      {
        n := (Number) v

        // Strip units
        if (n.unit != null)
        {
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
    x := that as Rec
    if (x == null) return false
    return (
      (id == x.id) &&
      (paths == x.paths) &&
      Etc.dictEq(values, x.values) &&
      (pathRefs == x.pathRefs) &&
      (spec == x.spec)
    )
  }

  override Str toStr() { "Rec($id)" }

  **
  ** Unique identifier
  **
  internal const Ref id

  **
  ** 'paths' containts the dotted path to every value.  Markers are stored here
  ** implicitly.
  **
  internal const Str[] paths

  **
  ** 'values' contains everything but Markers. If there are any Numbers, they
  ** have been stripped of their units.
  **
  internal const Dict values

  **
  ** The Path to each Ref, if there are any (could be empty).
  **
  internal const Str:Ref pathRefs

  **
  ** The spec, if there is one.
  **
  internal const Ref? spec
}
