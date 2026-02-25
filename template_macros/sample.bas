Attribute VB_Name = "Module1"
Sub z_efile_notings()

    Selection.WholeStory
    Selection.Font.Name = "Times New Roman"
    Selection.Font.Size = 12
    Selection.ParagraphFormat.LeftIndent = InchesToPoints(0)
    Selection.ParagraphFormat.RightIndent = InchesToPoints(0)
    Selection.ParagraphFormat.TabHangingIndent 0
    Selection.ParagraphFormat.FirstLineIndent = InchesToPoints(0)
    
    Selection.Find.ClearFormatting
    Selection.Find.Replacement.ClearFormatting
    
    With Selection.Find
        .Text = ":" & vbTab
        .Replacement.Text = ": "
        .Forward = True
        .Wrap = wdFindAsk
        .Format = False
        .MatchCase = False
        .MatchWholeWord = False
        .MatchKashida = False
        .MatchDiacritics = False
        .MatchAlefHamza = False
        .MatchControl = False
        .MatchWildcards = False
        .MatchSoundsLike = False
        .MatchAllWordForms = False
    End With
    Selection.Find.Execute Replace:=wdReplaceAll
    
    With Selection.Find
        .Text = "." & vbTab
        .Replacement.Text = ". "
        .Forward = True
        .Wrap = wdFindAsk
        .Format = False
        .MatchCase = False
        .MatchWholeWord = False
        .MatchKashida = False
        .MatchDiacritics = False
        .MatchAlefHamza = False
        .MatchControl = False
        .MatchWildcards = False
        .MatchSoundsLike = False
        .MatchAllWordForms = False
    End With
    Selection.Find.Execute Replace:=wdReplaceAll
    
    With Selection.Find
        .Text = "^t"
        .Replacement.Text = ""
        .Forward = True
        .Wrap = wdFindAsk
        .Format = False
        .MatchCase = False
        .MatchWholeWord = False
        .MatchKashida = False
        .MatchDiacritics = False
        .MatchAlefHamza = False
        .MatchControl = False
        .MatchWildcards = False
        .MatchSoundsLike = False
        .MatchAllWordForms = False
    End With
    Selection.Find.Execute Replace:=wdReplaceAll
    
'Sub table()

For Each t In ActiveDocument.Tables
   t.AutoFitBehavior wdAutoFitWindow
   t.Style = "Table Grid"
   t.Range.Font.Name = "Times New Roman"
   t.Range.Font.Size = 12
Next t

End Sub


