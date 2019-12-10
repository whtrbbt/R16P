using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;

namespace R16P
{
    class Program
    {
        static void Main(string[] args)
        {
            string rYear = ConfigurationManager.AppSettings.Get("YEAR");
            for (int i = 1; i<=1; i++)
            {
                FormR16P(rYear, i);
            }
            Console.WriteLine("Готово!");
            

        }

        public static void FormR16P(string year, int month)
        {
            DateTime date = new DateTime();
            DateTime sp = new DateTime();
            Decimal val = new Decimal();
            date = Convert.ToDateTime("01."+month+"."+year);
            
            
            
            string reestrName = "R16P";
            string fileName = @ConfigurationManager.AppSettings.Get("CSV_PATH");
            fileName += reestrName + "_" + date.ToString("MMyyyy") + ".csv";


            //Задаем формат чисел
            string specifier = "F2";
            CultureInfo culture = CultureInfo.CreateSpecificCulture("eu-ES");
            NumberFormatInfo nfi = culture.NumberFormat;
            nfi.NumberDecimalSeparator = ".";

            SqlConnectionStringBuilder csbuilder = new SqlConnectionStringBuilder("");

            csbuilder["Server"] = @ConfigurationManager.AppSettings.Get("MSSQL_Server");
            csbuilder["UID"] = @ConfigurationManager.AppSettings.Get("UID");
            csbuilder["Password"] = @ConfigurationManager.AppSettings.Get("Password");
            //csbuilder["Connect Timeout"] = 20000;
            csbuilder["integrated Security"] = true; //для коннекта с локальным экземпляром

            //Текст запроса к БД
            string reestrQuery;

            reestrQuery = $@"SELECT [ID]
                ,[DATE_INP]
                ,[VAL]
                ,[REMARK]
                ,[REESTR]
                ,(select distinct nomer from [ORACLE].[dbo].[FLS] where ID = FLS)[FLS]
                ,[PERIOD]
                ,[PAY_DATE]
                ,[IS_LOCKED]
                ,(SELECT DISTINCT [SERV_PERIOD] FROM [ORACLE].[dbo].[DOC_PAY_ITEM] WHERE [DOC_PAY] = [DOC_PAY].[ID]) AS SERV_PERIOD
                FROM [ORACLE].[dbo].[DOC_PAY]
                where DATE_INP >= '{date}' and DATE_INP < '{date.AddMonths(1)}' and VAL >= 0";

            //reestrQuery = $@"SELECT [ID]
            //    ,[DATE_INP]
            //    ,[VAL]
            //    ,[REMARK]
            //    ,[REESTR]
            //    ,(select distinct nomer from [ORACLE].[dbo].[FLS] where ID = FLS)[FLS]
            //    ,[PERIOD]
            //    ,[PAY_DATE]
            //    ,[IS_LOCKED]
            //    FROM [ORACLE].[dbo].[DOC_PAY]
            //    where 'ID' in (SELECT 'ID' FROM [ORACLE].[dbo].[DOC_PAY] where DATE_INP >= '{date}' and DATE_INP < '{date.AddMonths(1)}' and VAL >=0)";
            DataTable reestr = new DataTable();
            DataColumn column;
            DataRow reestrRow;


            #region Задаем структуру таблицы reestr
            //1. AccountOperator (ИНН оператора ЛС)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "AccoountOperator";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //2. AccountNum (Номер ЛС (ФЛС))
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "AccountNum";
            column.AllowDBNull = false;
            reestr.Columns.Add(column);

            //3. ServiceCode (Код услуги)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "ServiceCode";
            column.AllowDBNull = false;
            column.DefaultValue = "22";
            reestr.Columns.Add(column);

            //4. ProviderCode (ИНН поставщика услуг)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "ProviderCode";
            column.AllowDBNull = false;
            column.DefaultValue = "5190996259";
            reestr.Columns.Add(column);

            //5. PaySum (Сумма платежа по взносам в фонд капитального ремонта)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "PaySum";
            column.AllowDBNull = false;
            reestr.Columns.Add(column);

            //6. PayFineSum (Сумма платежа по пенни)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "PayFineSum";
            column.AllowDBNull = false;
            column.DefaultValue = 0;
            reestr.Columns.Add(column);

            //7. LastPayDate (Дата оплаты)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "LastPayDate";
            column.AllowDBNull = false;
            reestr.Columns.Add(column);

            //8. PayAgent (Код платежного агента)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "PayAgent";
            column.AllowDBNull = false;
            column.DefaultValue = "MR1010";
            reestr.Columns.Add(column);

            //9. PayID (Код платежa)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "PayID";
            column.AllowDBNull = false;
            reestr.Columns.Add(column);

            //10. Comment (Комментарий)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "Comment";
            column.AllowDBNull = true;
            reestr.Columns.Add(column);
            #endregion

            //Подключаемся к БД и получаем данные для таблицы reestr
            Console.WriteLine("Подключаемся к БД");
            using(DataTable qr = new DataTable())
            {

                using(SqlConnection conn = new SqlConnection(csbuilder.ConnectionString))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand(reestrQuery, conn);
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(qr);
                    conn.Close();
                }

                //Наполняем таблицу reestr данными из запроса
                Console.WriteLine("Получаем данные");
                foreach(DataRow row in qr.Rows)
                {
                    sp = Convert.ToDateTime(row["SERV_PERIOD"]);
                    date = Convert.ToDateTime(row["PAY_DATE"]);
                    //if ()
                    
                    reestrRow = reestr.NewRow();

                    //Номер ЛС
                    reestrRow["AccountNum"] = Convert.ToString(row["FLS"]);

                    //Текущие платежи
                    val = Convert.ToDecimal(row["VAL"]);
                    reestrRow["PaySum"] = val.ToString(specifier, nfi);

                    //Дата платежа                    
                    reestrRow["LastPayDate"] = date.ToString("yyyy-MM-dd");

                    //Код платежа
                    reestrRow["PayID"] = Convert.ToString(row["ID"]);

                    //Комментарии
                    date = Convert.ToDateTime(row["PERIOD"]);
                    reestrRow["Comment"] = "За период: " + date.ToString("yyyy-MM-dd") + "," + Convert.ToString(row["REMARK"]);

                    reestr.Rows.Add(reestrRow);
                }
                ////Проверка полученных значений
                //foreach(DataRow r in reestr.Rows)
                //{
                //    Console.WriteLine(Convert.ToString(r["AccountNum"])
                //        + " " + Convert.ToString(r["ServiceCode"])
                //        + " " + Convert.ToString(r["ProviderCode"])
                //        + " " + Convert.ToString(r["PaySum"])
                //        + " " + Convert.ToString(r["PayFineSum"])
                //        + " " + Convert.ToString(r["LastPayDate"])
                //        + " " + Convert.ToString(r["PayAgent"])
                //        + " " + Convert.ToString(r["PayID"])
                //        + " " + Convert.ToString(r["Comment"])
                //        );
                //}


                Console.WriteLine("Сохраняем отчет в CSV");
                ToCSV(reestr, fileName);




            }



        }

        public static void ToCSV(DataTable dtDataTable, string strFilePath)
        {
            StreamWriter sw = new StreamWriter(strFilePath, false);

            //Заголовок реестра
            string header = "#RTYPE=R16P\n"+
                "#AccountOperator;AccountNum;ServiceCode;ProviderCode;PaySum;PayFineSum;LastPayDate;PayAgent;PayID;SpecAccount;Comment";
            sw.Write(header);

            //Паттерн для поиска разделителя в полях таблицы
            string pattern = ";+";

            ////Выводим имена столбцов
            //for(int i = 0; i < dtDataTable.Columns.Count; i++)
            //{
            //    sw.Write(dtDataTable.Columns[i]);
            //    if(i < dtDataTable.Columns.Count - 1)
            //    {
            //        sw.Write(";");
            //    }
            //}
            
            //Выводим данные
            sw.Write(sw.NewLine);
            foreach(DataRow dr in dtDataTable.Rows)
            {
                for(int i = 0; i < dtDataTable.Columns.Count; i++)
                {
                    if(!Convert.IsDBNull(dr[i]))
                    {
                        string value = dr[i].ToString();
                        //if (value.Contains(';'))
                        //{
                        //value = String.Format("\\{0}\\", value);
                        value = Regex.Replace(value, @"\n+", " ");
                        value = Regex.Replace(value, pattern, ":");
                        sw.Write(value);
                        //}
                        //else
                        //{
                        //    sw.Write(dr[i].ToString());
                        //}
                    }
                    else if(Convert.IsDBNull(dr[i]))
                    {
                        sw.Write("NULL");
                    }
                    if(i < dtDataTable.Columns.Count - 1)
                    {
                        sw.Write(";");
                    }
                }
                sw.Write(sw.NewLine);
            }
            sw.Close();
        }

    }
}
