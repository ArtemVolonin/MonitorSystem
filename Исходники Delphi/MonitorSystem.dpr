program MonitorSystem;

uses
  Vcl.Forms,
  UnMainForm in 'UnMainForm.pas' {frmMainForm};

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.Title := '������� ���������� ��� ����������';
  Application.CreateForm(TfrmMainForm, frmMainForm);
  Application.Run;
end.
