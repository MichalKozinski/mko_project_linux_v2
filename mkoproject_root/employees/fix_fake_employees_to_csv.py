import csv 
MONTH = {'Jan':'01', 'Feb':'02', 'mar':'03', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}
FILENAME = 'employee_data_mko.csv'
FIXED_FILENAME = 'fixed_employees_data.csv'
SEPARATOR = ','

def fix_date(date_string):
    for key in MONTH:
        date_string=date_string.replace(key,MONTH[key])
    date_string=date_string.replace('.', '-')
    buffer = date_string.split(sep='-')
    if(len(buffer[2])==2):
        date_string = '20' + buffer[2] +  '-' + buffer[1] + '-' + buffer[0] 
    else:
        date_string = buffer[2] +  '-' + buffer[1] + '-' + buffer[0]
    return date_string


def open_and_fix_save():
    with open(FILENAME, newline='') as csvfile:
        content=csv.DictReader(csvfile)
        for row in content:
            if row['StartDate']:
                row['StartDate']=fix_date(row['StartDate'])
            if len(row['ExitDate'])>0:
                row['ExitDate']=fix_date(row['ExitDate'])
            else:
                row['ExitDate'] = ''
            if row['DOB']:
                row['DOB']=fix_date(row['DOB'])
            fixed_content=''
            for key,value in row.items():
                if key == 'CurrentEmployeeRating':
                    fixed_content += value
                else:
                    fixed_content += value
                    fixed_content += SEPARATOR
            #to trzeba przerobić na write csv : Employees.objects.create(EmpID=row['EmpID'], FirstName=row['FirstName'], LastName=row['LastName'], StartDate=row['StartDate'], ExitDate=row['ExitDate'], Title=row['Title'], Supervisor=row['Supervisor'], ADEmail=row['ADEmail'], EmployeeStatus=row['EmployeeStatus'], EmployeeType=row['EmployeeType'], PayZone=row['PayZone'], EmployeeClassificationType=row['EmployeeClassificationType'], TerminationType=row['TerminationType'], TerminationDescription=row['TerminationDescription'], DepartmentType=row['DepartmentType'], Division=row['Division'], DOB=row['DOB'], State=row['State'], JobFunctionDescription=row['JobFunctionDescription'], GenderCode=row['GenderCode'], LocationCode=row['LocationCode'], RaceDesc=row['RaceDesc'], MaritalDesc=row['MaritalDesc'], PerformanceScore=row['PerformanceScore'], CurrentEmployeeRating=row['CurrentEmployeeRating'])
            with open(FIXED_FILENAME, 'a') as stream:
                stream.write(fixed_content)
                stream.write('\n')

def main():
    open_and_fix_save()


if __name__ == '__main__':
    main()