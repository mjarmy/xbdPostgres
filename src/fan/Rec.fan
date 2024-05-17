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
    Str:Str strs,
    Str:Float nums,
    Str:Str units)
  {
    this.id    = id
    this.paths = paths
    this.refs  = refs
    this.strs  = strs
    this.nums  = nums
    this.units = units
  }

  **
  ** Create a Rec by transforming a top-level Dict
  **
  internal static new fromDict(Dict dict)
  {
    paths := Str[,]
    refs := Str:Str[:]
    strs := Str:Str[:]
    nums := Str:Float[:]
    units := Str:Str[:]

    traverseDict(
      dict, Str[,], paths,
      refs, strs, nums, units)

    return Rec(
      dict.id.id, paths,
      refs, strs, nums, units)
  }

  private static Void traverseDict(
      Dict d,
      Str[] curPath,
      Str[] paths,
      Str:Str refs,
      Str:Str strs,
      Str:Float nums,
      Str:Str units)
  {
    d.each |val, key|
    {
      curPath.add(key)
      dotted := curPath.join(".")
      paths.add(dotted)

      // dict
      if (val is Dict)
      {
        traverseDict(
          val, curPath, paths,
          refs, strs, nums, units)
      }
      // Ref
      else if (val is Ref)
      {
        refs.add(dotted, ((Ref) val).id)
      }
      // Str
      else if (val is Str)
      {
        strs.add(dotted, val)
      }
      // Number
      else if (val is Number)
      {
        Number n := (Number) val
        nums.add(dotted, n.toFloat)
        units.add(dotted, n.unit == null ? "_" : n.unit.toStr)
      }

      curPath.removeAt(-1)
    }
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
      (id    == x.id) &&
      (paths == x.paths) &&
      (refs  == x.refs) &&
      (strs  == x.strs) &&
      (nums  == x.nums) &&
      (units == x.units)
    )
  }

  override Str toStr() { "Rec($id)" }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  internal const Str id
  internal const Str[] paths

  internal const Str:Str   refs
  internal const Str:Str   strs
  internal const Str:Float nums
  internal const Str:Str   units
}
