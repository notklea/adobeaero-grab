local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

local item_patterns = {
  ["^https?://cc%-api%-cp%.adobe%.io/api/v2/aero/assets/([0-9a-f%-]+)"]="api-asset",
  ["^https?://cc%-api%-cp%.adobe%.io/api/v2/aero/users/([a-zA-Z0-9_%-%.]+)"]="api-user",
  ["^https?://(pps%.services%.adobe%.com/api/profile/.+)"]="asset",
  ["^https?://behance%.net/([0-9a-zA-Z_%-%.]+)"]="b-user",
  ["^https?://www%.behance%.net/([0-9a-zA-Z_%-%.]+)"]="b-user",
}

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
    if string.match(item, "^[a-z]+%-user:") then
      local v = string.match(item, "^[^:]+:(.+)$")
      discover_item(target, "api-user:" .. v)
      discover_item(target, "b-user:" .. v)
    end
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs(item_patterns) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {["templates"]={}, ["ignore"]={}}
    new_item_type = found["type"]
    new_item_value = found["value"]
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)])
    or string.match(url, "^https?://[^/]*data[^/]*%.adobe%.io/.") then
    return true
  end

  if context["ignore"][url]
    or context["ignore"][string.match(url, "^([^%?]+)")] then
    return false
  end

  local skip = false
  for pattern, type_ in pairs(item_patterns) do
    match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if string.match(url, "^https?://adobeaero%.app%.link/[0-9a-zA-Z]+$") then
    if item_type == "asset" then
      return true
    end
    return false
  end

  if string.match(url, "^https?://ar%.adobe%.com/landing/%?id=") then
    if not parenturl
      or string.match(parenturl, "adobeaero%.app%.link") then
      return true
    end
    return false
  end
    

  if not string.match(url, "^https?://[^/]*adobe%.io")
    and not string.match(url, "^https?://[^/]*behance%.net/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  for _, pattern in pairs({
    "([0-9a-zA-Z%-_]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if string.match(url_, "^https?://cc%-api%-cp%.adobe%.io/api/v2/aero/")
      and not string.match(url_, "[%?&]api_key=Aero_Content_Service1") then
      url_ = set_new_params(url_, {["api_key"]="Aero_Content_Service1"})
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  local function join_tables(a, b)
    local result = {}
    for _, t in pairs({a, b}) do
      for k, v in pairs(t) do
        result[k] = v
      end
    end
    return result
  end

  local function queue_template(newurl, data)
    local found = false
    for k, v in pairs(data) do
      local newdata = join_tables(data, {})
      if v then
        newdata[k] = nil
        found = true
      end
      if type(v) == "string" then
        v = string.gsub(v, "([^0-9a-zA-Z])", "%%%1")
        queue_template(string.gsub(newurl, "{" .. k .. "}", v), newdata)
      elseif type(v) == "table" then
        for _, s in pairs(v) do
          s = string.gsub(s, "([^0-9a-zA-Z])", "%%%1")
          queue_template(string.gsub(newurl, "{" .. k .. "}", s), newdata)
        end
      end
    end
    if not found then
      if string.match(newurl, "{[a-z]+}") then
        error("Template not filled on " .. newurl .. ".")
      end
      check(newurl)
    end
  end

  local function process_manifest(manifest, path)
    local version = string.match(url, "([0-9]+)$")
    path = path or ""
    if manifest["path"] then
      path = path .. "/" .. manifest["path"]
    end
    local paths = {}
    if manifest["components"] then
      for _, d in pairs(manifest["components"]) do
        local new = path .. "/" .. d["path"]
        if string.match(new, "^/") then
          new = string.match(new, "^/(.+)$")
        end
        table.insert(paths, new)
      end
    end
    if manifest["children"] then
      for _, d in pairs(manifest["children"]) do
        process_manifest(d, path)
      end
    end
    for newurl, _ in pairs(context["templates"]) do
      queue_template(newurl, join_tables(
        context["default_params"],
        {
          ["version"] = version,
          ["path"] = paths
        }
      ))
    end
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "asset"
    and not string.match(url, "^https?://cdn%.cp%.adobe%.io/.-/path/")
    and not string.match(url, "^https?://cdn%.cp%.adobe%.io/content/2/dcx/[^/]+/content/[A-Z]")
    and not string.match(url, "^https?://[^/]*data[^/]*%.adobe%.io/.") then
    html = read_file(file)
    if string.match(url, "/api/v2/aero/assets/[^/]-%?") then
      json = cjson.decode(html)
      if json["type"] ~= "application/vnd.adobe.real+dcx" then
        error("Unsupported asset type found.")
      end
      context["default_params"] = {
        ["format"] = "jpg",
        ["dimension"] = "width",
        ["size"] = {"0", "200", "1200"},
        ["version"] = {}
      }
      for i = 0, tonumber(json["version"]) do
        table.insert(context["default_params"]["version"], tostring(i))
      end
      local found_content = false
      for _, d in pairs(json["_links"]) do
        if string.match(d["href"], "{") then
          context["ignore"][d["href"]] = true
        end
        if string.match(d["href"], "^https?://cdn%.cp%.adobe%.io/content/2/dcx/[^/]+/content$") then
          found_content = true
          context["templates"][d["href"] .. "/{path}"] = true
        end
        local newurl = string.gsub(d["href"], "/version/[0-9]+", "/version/{version}")
        if string.match(newurl, "{")
          and not (string.match(newurl, "{path}") and string.match(newurl, "/rendition/"))
          and not string.match(newurl, "^https?://cdn%.cp%.adobe%.io/content/2/dcx/[^/]+/content/{path}/version/{version}$") then
          if string.match(newurl, "{path}") then
            context["templates"][newurl] = true
          else
            queue_template(newurl, context["default_params"])
          end
        end
      end
      if not found_content then
        error("Could not find https://cdn.cp.adobe.io/content/2/dcx/[...]/content URL.")
      end
    end
    if string.match(url, "/content/2/dcx/[0-9a-f%-]+/content/manifest/version/[0-9]+$") then
      json = cjson.decode(html)
      if json["type"] ~= "application/vnd.adobe.real+dcx" then
        error("Found unsupported type " .. json["type"] .. ".")
      end
      process_manifest(json)
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if string.match(url["url"], "^https?://cc%-api%-cp%.adobe%.io/api/v2/aero/assets/[a-f0-9%-]+%?") then
    local html = read_file(http_stat["local_file"])
    if not string.match(html, "/content/manifest/version/[0-9]+\"") then
      retry_url = true
      return false
    end
  end
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 404
    and (item_type == "api-asset" and http_stat["statcode"] ~= 307)
    and (item_type == "api-user" and http_stat["statcode"] == 203) then
    retry_url = true
    return false
  end

  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 11
    if status_code == 401 or status_code == 403 then
      tries = maxtries + 1
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if status_code == 307 and item_type == "api-asset" and not allowed(newloc) then
      error("Data URL " .. newloc .. " should have been accepted.")
    end
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["adobeaero-87q4dwdfeojnu3wr"] = discovered_items,
    ["urls-han8wprk05vq9x2q"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


