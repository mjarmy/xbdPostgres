//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** SelectBuilder
**
const class SelectBuilder
{
  new make(Filter f)
  {
    this.select = Select("foo", Str[,])
  }

  const Select select
}
