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
    Str id,
    Str[] paths,
    Str:Str refs,
    Str:Str strs)
  {
    this.id    = id
    this.paths = paths
    this.refs  = refs
    this.strs  = strs
  }

  **
  ** Create a Rec by transforming a top-level Dict
  **
  internal static new fromDict(Dict dict)
  {
    paths := Str[,]
    refs := Str:Str[:]
    strs := Str:Str[:]

    traverseDict(
      dict,
      Str[,],
      paths,
      refs,
      strs)

    return Rec(
      dict.id.id,
      paths,
      refs,
      strs)
  }

  private static Void traverseDict(
      Dict d,
      Str[] curPath,
      Str[] paths,
      Str:Str refs,
      Str:Str strs)
  {
    d.each |v,k|
    {
      curPath.add(k)
      dotted := curPath.join(".")
      paths.add(dotted)

      // dict
      if (v is Dict)
      {
        traverseDict(v, curPath, paths, refs, strs)
      }
      // Ref
      else if (v is Ref)
      {
        refs.add(dotted, makeQueryable(v))
      }
      // Str
      else if (v is Str)
      {
        strs.add(dotted, makeQueryable(v))
      }

      curPath.removeAt(-1)
    }
  }

  internal static Obj makeQueryable(Obj val)
  {
    if (val is Ref)
    {
      return ((Ref) val).id
    }
    else if (val is Str)
    {
      return val
    }
    else throw Err("Unrecognized scalar: $val (${val.typeof})")
  }

//////////////////////////////////////////////////////////////////////////
// Obj
//////////////////////////////////////////////////////////////////////////

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
      (refs == x.refs) &&
      (strs == x.strs)
    )
  }

  override Str toStr() { "Rec($id)" }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  internal const Str id
  internal const Str[] paths

  internal const Str:Str refs
  internal const Str:Str strs
}
