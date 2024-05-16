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
  ** Only used for unit tests
  **
  internal new make(
    Ref id,
    Str[] paths,
    Dict hayson,
    Str:Ref pathRefs,
    Ref? spec)
  {
    this.id       = id
    this.paths    = paths
    this.hayson   = hayson
    this.pathRefs = pathRefs
    this.spec     = spec
  }

  **
  ** Create a Rec by transforming a Dict
  **
  internal static new fromDict(Dict dict)
  {
    paths := Str[,]
    hayson := Str:Obj[:]
    pathRefs := Str:Ref[:]

    traverseDict(dict, Str[,], paths, hayson, pathRefs)

    return Rec(
      dict.id,
      paths,
      Etc.makeDict(hayson),
      pathRefs,
      dict.get("spec", null))
  }

  private static Void traverseDict(
      Dict d, Str[] curPath, Str[] paths,
      Str:Obj hayson, Str:Ref pathRefs)
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
          hayson.add(k, Etc.makeDict(nvalues))
      }
      // Ref
      else if (v is Ref)
      {
        pathRefs.add(cp, v)
        hayson.add(k, v)
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
        hayson.add(k, n)
      }
      // DateTime
      else if (v is DateTime)
      {
        dt := (DateTime) v

        // use "fantom epoch millis"
        hayson.add(k, Etc.dict2(
          "_kind", "dateTime",
          "millis", Duration(dt.ticks).toMillis))
      }
      // remove markers
      else if (!(v is Marker))
      {
        hayson.add(k, v)
      }

      curPath.removeAt(-1)
    }
  }

  **
  ** Only used for unit tests
  **
  override Int hash() { id.hash }

  **
  ** Only used for unit tests
  **
  override Bool equals(Obj? that)
  {
    x := that as Rec
    if (x == null) return false
    return (
      (id == x.id) &&
      (paths == x.paths) &&
      Etc.dictEq(hayson, x.hayson) &&
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
  ** 'paths' contains the dotted path to every value.  Markers are stored here
  ** implicitly.
  **
  internal const Str[] paths

  **
  ** 'hayson' contains everything but Markers. If there are any Numbers, they
  ** have been stripped of their units.
  **
  internal const Dict hayson

  **
  ** The Path to each Ref, if there are any (could be empty).
  **
  internal const Str:Ref pathRefs

  **
  ** The spec, if there is one.
  **
  internal const Ref? spec
}
