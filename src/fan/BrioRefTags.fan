//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using concurrent
using haystack

**
** BrioRefTags defines the Set of brio tags that end in "Ref" or "Of"
**
const class BrioRefTags
{
  static BrioRefTags cur() { curRef.val }
  private static const AtomicRef curRef := AtomicRef(null)

  static
  {
    try
    {
      if (Env.cur.runtime != "js")
        curRef.val = load()
    }
    catch (Err e)
      e.trace
  }

  private static BrioRefTags load()
  {
    tags := [Str:Str][:]

    BrioConsts.cur.byVal.each |v,k|
    {
      if ((k != "Ref" && k.endsWith("Ref")) || k.endsWith("Of"))
        tags[k] = k
    }

    return make
    {
      it.tags = tags
    }
  }

  private new make(|This| f) { f(this) }

  //-----------------------------------------------
  // Fields
  //-----------------------------------------------

  **
  ** The Set of brio tags that end in "Ref" or "Of"
  **
  const [Str:Str] tags
}
