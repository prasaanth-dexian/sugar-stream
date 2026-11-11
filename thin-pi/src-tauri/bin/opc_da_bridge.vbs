' Copyright (c) 2026 Dexian India Technologies Private Limited.
' All rights reserved.
'
' This file is proprietary and confidential. Unauthorised copying,
' distribution or use of this file, via any medium, is strictly
' prohibited without the prior written permission of Dexian India
' Technologies Private Limited.

' ==============================================================================
' THIN PI — Industrial OPC DA 2.05a COM Bridge Worker
' ==============================================================================
' Usage:
'   cscript.exe //Nologo opc_da_bridge.vbs --browse [ProgID]
'   cscript.exe //Nologo opc_da_bridge.vbs --stream [ProgID] [Tag1,Tag2,...]
' ==============================================================================

Option Explicit

Dim args, mode, progId, tagsArg
Set args = WScript.Arguments

If args.Count = 0 Then
    mode = "--stream"
    progId = "Matrikon.OPC.Simulation.1"
    tagsArg = ""
Else
    mode = args(0)
    If args.Count > 1 Then
        progId = args(1)
    Else
        progId = "Matrikon.OPC.Simulation.1"
    End If
    If args.Count > 2 Then
        tagsArg = args(2)
    Else
        tagsArg = ""
    End If
End If

On Error Resume Next

Dim opc
Set opc = CreateObject("Matrikon.OPC.Automation.1")
If Err.Number <> 0 Then
    ' Fallback to OPC Foundation generic automation wrapper if Matrikon wrapper is absent
    Err.Clear
    Set opc = CreateObject("OPC.Automation.1")
End If

If Err.Number <> 0 Then
    WScript.Echo "{""status"":""error"",""message"":""Failed to instantiate OPC Automation COM wrapper: " & Replace(Err.Description, """", "\""") & """}"
    WScript.Quit 1
End If

opc.Connect progId
If Err.Number <> 0 Then
    WScript.Echo "{""status"":""error"",""message"":""Failed to connect to OPC DA server '" & progId & "': " & Replace(Err.Description, """", "\""") & """}"
    WScript.Quit 1
End If

On Error GoTo 0

If mode = "--browse" Then
    DoBrowse opc, progId
Else
    DoStream opc, progId, tagsArg
End If

Sub DoBrowse(opcObj, serverProgId)
    On Error Resume Next
    Dim browser
    Set browser = opcObj.CreateBrowser()
    
    Dim tagsJson
    tagsJson = ""
    
    browser.ShowLeafs True
    Dim tagItem, count
    count = 0
    For Each tagItem In browser
        If count < 100 Then
            Dim jsonNode
            jsonNode = "{""node_id"":""" & tagItem & """,""name"":""" & tagItem & """,""eng_unit"":""""}"
            If tagsJson <> "" Then
                tagsJson = tagsJson & ","
            End If
            tagsJson = tagsJson & jsonNode
            count = count + 1
        End If
    Next
    
    WScript.Echo "{""status"":""browse_success"",""server"":""" & serverProgId & """,""tags"":[" & tagsJson & "]}"
    opcObj.Disconnect
    WScript.Quit 0
End Sub

Sub DoStream(opcObj, serverProgId, selectedTags)
    Dim tagNames
    If selectedTags = "" Then
        tagNames = Array( _
            "Random.Int4", _
            "Random.Real4", _
            "Random.Real8", _
            "Bucket Brigade.Int4", _
            "Bucket Brigade.Real4", _
            "Money.Real8" _
        )
    Else
        tagNames = Split(selectedTags, ",")
    End If

    Dim groups, group, items
    Set groups = opcObj.OPCGroups
    Set group = groups.Add("ThinPiStreamGroup")
    group.IsActive = True
    group.UpdateRate = 250

    Set items = group.OPCItems

    Dim itmList(), validCount
    ReDim itmList(UBound(tagNames))
    validCount = 0

    Dim i
    For i = 0 To UBound(tagNames)
        On Error Resume Next
        Dim tName
        tName = Trim(tagNames(i))
        If tName <> "" Then
            Dim itm
            Set itm = items.AddItem(tName, i + 1)
            If Err.Number = 0 And Not itm Is Nothing Then
                Set itmList(validCount) = itm
                validCount = validCount + 1
            End If
        End If
        On Error GoTo 0
    Next

    If validCount = 0 Then
        WScript.Echo "{""status"":""error"",""message"":""No valid OPC DA tags could be registered""}"
        opcObj.Disconnect
        WScript.Quit 1
    End If

    ' Send Ready Handshake
    WScript.Echo "{""status"":""connected"",""server"":""" & serverProgId & """,""tag_count"":" & validCount & "}"

    WScript.Sleep 300

    Do While True
        Dim nowMs
        nowMs = DateDiff("s", "01/01/1970 00:00:00", Now()) * 1000
        
        Dim jsonParts
        jsonParts = ""
        
        Dim j
        For j = 0 To validCount - 1
            Dim v, q, t
            itmList(j).Read 1, v, q, t
            If IsNumeric(v) Then
                Dim part
                part = "{""tag"":""" & itmList(j).ItemID & """,""value"":" & v & ",""quality"":" & q & ",""timestamp"":" & nowMs & "}"
                If jsonParts <> "" Then
                    jsonParts = jsonParts & ","
                End If
                jsonParts = jsonParts & part
            End If
        Next
        
        If jsonParts <> "" Then
            WScript.Echo "[" & jsonParts & "]"
        End If
        
        WScript.Sleep 250
    Loop

    opcObj.Disconnect
End Sub
