import org.hibernate.*;
import org.hibernate.cfg.Configuration;

import java.util.Iterator;
import java.util.List;

public class Main {

    private static SessionFactory factory;

    public static void main(String[] args) {
        try {
            factory = new Configuration().configure().buildSessionFactory();
        } catch (Throwable ex) {
            System.err.println("Failed to create sessionFactory object." + ex);
            throw new ExceptionInInitializerError(ex);
        }

        Main main = new Main();

        Integer empID1 = main.addEmployee("fname", "lastName", 999);
        Integer empID2 = main.addEmployee("fname2", "lastName3", 999);
        /* List down all the employees */
        main.listEmployees();

        /* Update employee's records */
        main.updateEmployee(empID1, 5000);

        /* Delete an employee from the database */
        main.deleteEmployee(empID2);

        /* List down new list of the employees */
        main.listEmployees();

        //
        Employee employee = main.getEmployeeById(1);

        System.out.println("employee id ==> "+employee.getId()+" emplye name " + employee.getFirstName());

        main.addEmployees();
    }

    public Integer addEmployee(String fname, String lname, int salary){
        Session session = factory.openSession();
        Transaction tx = null;
        Integer employeeID = null;

        try {
            tx = session.beginTransaction();
            Employee employee = new Employee(fname, lname, salary);
            employeeID = (Integer) session.save(employee);
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        return employeeID;
    }

    /* Method to  READ all the employees */
    public void listEmployees( ){
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            List employees = session.createQuery("FROM Employee").list();
            for (Iterator iterator = employees.iterator(); iterator.hasNext();){
                Employee employee = (Employee) iterator.next();
                System.out.print("First Name: " + employee.getFirstName());
                System.out.print("  Last Name: " + employee.getLastName());
                System.out.println("  Salary: " + employee.getSalary());
            }
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    /* Method to UPDATE salary for an employee */
    public void updateEmployee(Integer EmployeeID, int salary ){
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Employee employee = (Employee)session.get(Employee.class, EmployeeID);
            employee.setSalary( salary );
            session.update(employee);
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    /* Method to DELETE an employee from the records */
    public void deleteEmployee(Integer EmployeeID){
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Employee employee = (Employee)session.get(Employee.class, EmployeeID);
            session.delete(employee);
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public Employee getEmployeeById(Integer EmployeeID){
        Session session = factory.openSession();
        Transaction tx = null;
        Employee employee = null;
        try {
            tx = session.beginTransaction();
            Query query = session.createQuery("from Employee e where e.id =:employee_id");
            query.setParameter("employee_id",EmployeeID);
            query.setCacheable(true);
            query.setCacheRegion("employee");
             employee = (Employee) query.list().get(0);
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        return employee;
    }


    /* Method to create employee records in batches */
    public void addEmployees( ){
        Session session = factory.openSession();
        Transaction tx = null;
        Integer employeeID = null;

        try {
            tx = session.beginTransaction();
            for ( int i=0; i<100000; i++ ) {
                String fname = "First Name " + i;
                String lname = "Last Name " + i;
                Integer salary = i;
                Employee employee = new Employee(fname, lname, salary);
                session.save(employee);
                if( i % 50 == 0 ) {
                    session.flush();
                    session.clear();
                }
            }
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }
}
