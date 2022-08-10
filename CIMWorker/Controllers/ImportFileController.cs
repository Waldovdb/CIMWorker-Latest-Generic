#region [ using ]
using CIMWorker.Data.Entities;
using CIMWorker.Models;
using CIMWorker.Services;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
#endregion

namespace CIMWorker.Controllers
{
    public class ImportFileController
    {
        private readonly IDataService _dataService;
        private readonly IFileService _fileService;
        private readonly ILogService _logService;
        private readonly IEmailService _emailService;
        private readonly IDiallerService _dialerService;
        private readonly DateTime _startTime;

        #region [ Default Constructor ]
        public ImportFileController(IDataService dataService, IFileService fileService, ILogService logService, IEmailService emailService, IDiallerService dialerService)
        {
            _dataService = dataService;
            _fileService = fileService;
            _logService = logService;
            _emailService = emailService;
            _dialerService = dialerService;
            _startTime = DateTime.Now;
        }
        #endregion

        //-----------------------------//

        #region [ Master Async ]
        public async Task<bool> MasterAsync()
        {
            int Pass = 0, Fail = 0;
            int PassTotal = 0, FailTotal = 0;
            bool hasFiles = false;
            DateTime start = DateTime.Now;
            await _logService.InfoAsync("Import File Controller Started");
            try
            {
                var fileConfigs = await _fileService.GetFileConfigurations();
                fileConfigs = fileConfigs.Where(o => o.IsActive == true).ToList();
                for (int i = 0; i < fileConfigs.Count(); i++)
                {
                    var files = await _fileService.GetFileList(fileConfigs[i].Location, fileConfigs[i].Partial);
                    //var files = await _fileService.GetFileList("C:\\0.InovoCIM\\Busy", fileConfigs[i].Partial);

                    for (int x = 0; x < files.Count(); x++)
                    {
                        FileInfo file = files[x];
                        bool IsHeaderValidate = await _fileService.ValidateHeader(file, fileConfigs[i]);
                        if (IsHeaderValidate)
                        {
                            DataTable FileTable = new DataTable();

                            FileTable = await _fileService.SetDataTableSchema(fileConfigs[i].TableName);
                            FileTable = await ReadTextFile(FileTable, fileConfigs[i], file);
                            int InstanceID = 0;
                            if (FileTable.Rows.Count >= 1)
                            {
                                hasFiles = true;
                                bool IsFileLogged = _fileService.LogFileDataTable(FileTable, fileConfigs[i].TableName);
                                if (!String.IsNullOrEmpty(fileConfigs[i].PersonIDNumber) && !String.IsNullOrEmpty(fileConfigs[i].PersonExternalID))
                                {
                                    foreach (DataRow row in FileTable.Rows)
                                {
                                    var person = new Person();
                                    #region [ Person Contacts ]
                                    Dictionary<int, PersonContact> contacts = new Dictionary<int, PersonContact>();
                                    if(!String.IsNullOrEmpty(fileConfigs[i].PersonPhone1))
                                    {
                                        contacts.Add(1, new PersonContact(row[fileConfigs[i].PersonPhone1].ToString()));
                                    }
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone2))
                                    {
                                        contacts.Add(2, new PersonContact(row[fileConfigs[i].PersonPhone2].ToString()));
                                    }
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone3))
                                    {
                                        contacts.Add(3, new PersonContact(row[fileConfigs[i].PersonPhone3].ToString()));
                                    }
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone4))
                                    {
                                        contacts.Add(4, new PersonContact(row[fileConfigs[i].PersonPhone4].ToString()));
                                    }           
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone5))
                                    {
                                        contacts.Add(5, new PersonContact(row[fileConfigs[i].PersonPhone5].ToString()));
                                    }
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone6))
                                    {
                                        contacts.Add(6, new PersonContact(row[fileConfigs[i].PersonPhone6].ToString()));
                                    }
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone7))
                                    {
                                        contacts.Add(7, new PersonContact(row[fileConfigs[i].PersonPhone7].ToString()));
                                    }
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone8))
                                    {
                                        contacts.Add(8, new PersonContact(row[fileConfigs[i].PersonPhone8].ToString()));
                                    }
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone9))
                                    {
                                        contacts.Add(9, new PersonContact(row[fileConfigs[i].PersonPhone9].ToString()));
                                    }
                                    if (!String.IsNullOrEmpty(fileConfigs[i].PersonPhone10))
                                    {
                                        contacts.Add(10, new PersonContact(row[fileConfigs[i].PersonPhone10].ToString()));
                                    }
                                    #endregion
                                    int Title = 0;
                                    int SourceID = 0;
                                    //int ServiceID = 0;
                                    //int LoadID = 0;
                                    //int Priority = 0;
                                    if(String.IsNullOrEmpty(fileConfigs[i].PersonTitle))
                                    {
                                        Title = 0;
                                    }

                                        person = new Person(Title, row[fileConfigs[i].PersonName].ToString(), row[fileConfigs[i].PersonSurname].ToString(), row[fileConfigs[i].PersonIDNumber].ToString(), row[fileConfigs[i].PersonExternalID].ToString());
                                        SourceID = await GetSourceID(person, contacts);
                                        //ServiceID = GetServiceID(fileConfigs[i], row);
                                        //LoadID = GetLoadID(fileConfigs[i], row);
                                        //Priority = await GetPriority(fileConfigs[i], row);

                                        //PhoneQueue singleQueue = new PhoneQueue();

                                        //try
                                        //{
                                        //    //Dictionary<int, PersonContact> contacts = new Dictionary<int, PersonContact>();
                                        //    //contacts.Add(1,personContact);
                                        //    //contacts.Add(2,personContact2);
                                        //    //contacts.Add(3,personContact3);
                                        //    //singleQueue = MapFileToPhoneQueue(SourceID, ServiceID, LoadID, Priority, row, file, person, contacts);

                                        //    if(singleQueue.Command != null)
                                        //    {
                                        //        phoneQueue.Add(singleQueue);
                                        //    }
                                        //}
                                        //catch (Exception ex)
                                        //{

                                        //}
                                    }
                                }
                                //bool IsLoaded = await _fileService.AddToQueue(phoneQueue);
                                await _fileService.CloseTextFile(file);
                                await _dataService.UpdateSingle<dynamic, dynamic>(fileConfigs[i].OnLoadedQuery, new { });
                                PassTotal += Pass;
                                FailTotal += Fail;
                            }
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                await _logService.ErrorAsync(GetType().Name, MethodBase.GetCurrentMethod(), ex);
            }

            TimeSpan runSpan = DateTime.Now.Subtract(_startTime);
            double RecordsPerSecond = ((Pass + Fail) / runSpan.TotalSeconds);
            await _logService.InfoAsync($"Import File Controller Ended : Runtime -> Records per Second: {RecordsPerSecond}");

            return true;
        }
        #endregion

        #region [ Read Text File ]
        public async Task<DataTable> ReadTextFile(DataTable FileTable, ImportDataFile model, FileInfo file)
        {
            DateTime StartFile = DateTime.Now;
            try
            {
                int LineID = 0, Pass = 0, Fail = 0;
                string FileLine;
                using (var fileStream = File.OpenRead(file.FullName))
                using (var reader = new StreamReader(fileStream, Encoding.UTF8, true, 1024))
                {
                    while ((FileLine = await reader.ReadLineAsync()) != null)
                    {
                        if(!String.IsNullOrEmpty(FileLine))
                        {

                        LineID++;
                        try
                        {
                            string[] InData = null;
                            if (model.HasHeader == false)
                            {
                                InData = await _fileService.GetDelimiterLine(FileLine, model, LineID);
                            }
                            else
                            {
                                model.HasHeader = false;
                                continue;
                            }

                            if (InData.Count() >= 1)
                            {
                                FileTable = await AddToFileTable(FileTable, InData, LineID);
                                Pass++;
                            }
                            else
                            {
                                Fail++;
                            }
                        }
                        catch (Exception ex)
                        {
                            continue;
                        }
                    }

                    }
                }

                TimeSpan runtime = DateTime.Now.Subtract(StartFile);
                await _logService.InfoAsync($"Read Text File - Lines: { LineID} || Pass: { Pass} || Fail: { Fail} || Time: { runtime.ToString()}");
            }
            catch (Exception ex)
            {
                await _logService.ErrorAsync(GetType().Name, MethodBase.GetCurrentMethod(), ex);
            }
            return FileTable;
        }
        #endregion

        #region [ Add To File Table ]
        public async Task<DataTable> AddToFileTable(DataTable Table, string[] InData, int LineID)
        {
            DataRow Row = Table.NewRow();

            int ColumnIndex = -1;
            bool IsLineError = false;
            foreach (DataColumn column in Table.Columns)
            {
                IsLineError = false;
                try
                {
                    ColumnIndex++;
                    Row["CIMID"] = DBNull.Value;
                    Row["InsertedDT"] = DBNull.Value;
                    Row["Completed"] = DBNull.Value;
                    if (ColumnIndex >= 3)
                    {
                        string item = column.DataType.ToString();
                        if (item == "System.Decimal")
                        {
                            var value = string.IsNullOrEmpty(InData[ColumnIndex - 1].ToString()) ? "0" : InData[ColumnIndex - 1];
                            value = (value.Contains('.')) ? value.Substring(0, value.IndexOf('.')) : value;
                            Row[ColumnIndex] = Convert.ToDecimal(value);
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(InData[ColumnIndex - 3].ToString()))
                            {
                                Row[ColumnIndex] = DBNull.Value;
                            }
                            else
                            {
                                Row[ColumnIndex] = InData[ColumnIndex - 3];
                            }
                        }
                    }
                }
                catch (Exception ex)
                 {
                    IsLineError = true;
                    continue;
                }
            }

            if (IsLineError == false)
            {
                Table.Rows.Add(Row);
            }


            return Table;
        }
        #endregion


        #region [ Map File To Phone Queue ]
        public PhoneQueue MapFileToPhoneQueue(int SourceID, int ServiceID, int LoadID, int Priority, DataRow row, FileInfo file, Person person, Dictionary<int,PersonContact> contacts)
        {
            PhoneQueue phoneQueue = new PhoneQueue();
            phoneQueue.Command = "addcall";
            phoneQueue.Input = "File";
            phoneQueue.InputName = file.Name;
            phoneQueue.Status = "Received";
            phoneQueue.Received = DateTime.Now;
            phoneQueue.NextExecute = DateTime.Now;
            phoneQueue.RetryCount = 0;
            phoneQueue.PersonID = SourceID;
            switch (ServiceID)
            {
                default:
                    phoneQueue.ServiceID = ServiceID;
                    break;
            }
            phoneQueue.SourceID = SourceID;
            phoneQueue.ServiceID = ServiceID;
            phoneQueue.LoadID = LoadID;
            phoneQueue.Name = person.Name;
            phoneQueue.Name = (phoneQueue.Name.Length > 40) ? phoneQueue.Name.Substring(0, 39) : phoneQueue.Name;
            phoneQueue.Phone = contacts[1].Contact;
            phoneQueue.Priority = Priority;
            phoneQueue.CapturingAgent = 0;
            phoneQueue.Phone1 = contacts[1].Contact;
            phoneQueue.Phone2 = contacts[2].Contact;
            phoneQueue.Phone3 = contacts[3].Contact;
            switch (ServiceID)
            {
                default:
                    phoneQueue.CustomData1 = null;
                    break;
            }
            switch (ServiceID)
            {
                default:
                    phoneQueue.CustomData2 = null;
                    break;
            }
            switch (ServiceID)
            {
                default:
                    phoneQueue.CustomData3 = null;
                    break;
            }

            return phoneQueue;
        }
        #endregion

        //-----------------------------//

        #region [ Get ServiceID ]
        private int GetServiceID(ImportDataFile model, DataRow row)
        {
            return 1;
        }
        #endregion

        #region [ Get LoadID ]
        private int GetLoadID(ImportDataFile model, DataRow row) // Update this with logic for Telemarketing Load Creation
        {
            return 1;
        }
        #endregion

        #region [ Get LoadID ]
        private int GetLoadID(ImportDataFile model, DataRow row, int ServiceID) 
        {
            return 1;
        }
        #endregion

        #region [ Get Priority ]
        private async Task<int> GetPriority(ImportDataFile model, DataRow row)
        {
            try
            {
                int MaxPriority = 0;
                return MaxPriority + 1;
            }
            catch (Exception)
            {
                return 1;
            }
        }
        #endregion

        #region [ Get SourceID ]
        private async Task<int> GetSourceID(Person data, Dictionary<int,PersonContact> contacts)
        {
            int PersonID = -1;
            try
            {
                string query = @"IF NOT EXISTS(SELECT TOP 1 [PersonID] FROM [dbo].[Person] WHERE [ExternalID] = @ExternalID)
                           BEGIN
	                           INSERT INTO [dbo].[Person] ([Title],[Name],[Surname],[IDNumber],[ExternalID],[Updated]) VALUES (1,@Name,@Surname,@IDNumber,@ExternalID,GETDATE())
                           END";

                await _dataService.InsertSingle<Person, Person>(query, data);
                var person = await _dataService.SelectSingle<Person, dynamic>("SELECT TOP 1 * FROM [Person] WHERE [ExternalID] = @ExternalID", new { data.ExternalID });
                if (person != null)
                {
                    PersonID = person.PersonID;

                    string phoneQuery = @"IF NOT EXISTS(SELECT TOP 1 [PersonID] FROM [dbo].[PersonContact] WHERE [Contact] = @Contact)
                                     BEGIN
	                                     INSERT INTO [dbo].[PersonContact] ([PersonID],[Type],[Contact],[Created]) VALUES (@PersonID,1,@Contact,GETDATE())
                                     END";

                    foreach(var dataContact in contacts)
                    {
                        dataContact.Value.PersonID = PersonID;
                        await _dataService.InsertSingle<PersonContact, PersonContact>(phoneQuery, dataContact.Value);
                    }
                }
            }
            catch (Exception ex)
            {
                await _logService.ErrorAsync(GetType().Name, MethodBase.GetCurrentMethod(), ex);
            }

            return PersonID;
        }
        #endregion
    }
}