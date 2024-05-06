//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

class SelectTest : Test
{
  Void testSelect()
  {
    echo("Hello from testSelect")

    verifyEq(
      SelectBuilder(Filter("a")).select,
      Select("foo", Str[,]))
  }
}
