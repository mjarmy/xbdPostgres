#! /usr/bin/env fan
//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   19 Mar 2024  Mike Jarmy  Creation
//

using build

**
** Build: xbdPostgres
**
class Build : BuildPod
{
  new make()
  {
    podName = "xbdPostgres"
    summary = "xbd Postgres experiment"
    meta    = ["org.name":     "XetoBase",
               "org.uri":      "https://xetobase.com/",
               "license.name": "Commercial"]
    depends = ["sys @{fan.depend}",
               "concurrent @{fan.depend}",
               "sql @{fan.depend}",
               "util @{fan.depend}",
               "haystack @{hx.depend}",
               "xeto @{hx.depend}",
              ]
    srcDirs = [`fan/`, `test/`]
  }
}
