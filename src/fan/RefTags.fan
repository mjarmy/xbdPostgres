//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using concurrent
using haystack

const class RefTags
{
  static RefTags cur() { curRef.val }
  private static const AtomicRef curRef := AtomicRef(null)

  // Set of brio tags that end in "Ref" or "Of"
  const [Str:Str] tags

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

  private static RefTags load()
  {
    echo("RefTags.load")

    tags := [Str:Str][:]

    return make
    {
      it.tags = tags
    }
  }

  private new make(|This| f) { f(this) }
}
