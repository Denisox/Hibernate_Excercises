import entities.*;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.SQLOutput;
import java.util.List;
import java.util.Set;

public class Engine implements Runnable{

    private final EntityManager entityManager;
    private BufferedReader bufferedReader;

    public Engine(EntityManager entityManager) {
        this.entityManager = entityManager;
        this.bufferedReader = new BufferedReader(new InputStreamReader(System.in));
            }

    @Override
    public void run() {
        System.out.println("Select excercise number:");
        try {
            int exNum = Integer.parseInt(bufferedReader.readLine());
            switch(exNum){
                case 2:
                    exTwo();
                case 3:
                    exThree();
                case 4:
                    exFour();
                case 5:
                    exFive();
                case 6:
                    exSix();
                case 7:
                    exSeven();
                case 8:
                    exEight();
                case 9:
                    exNine();
                case 10:
                    exTen();
                case 11:
                    exEleven();
                case 12:
                    exTwelve();
                case 13:
                    exThirteen();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            entityManager.close();
        }


    }

    private void exThirteen() throws IOException {
        System.out.println("Enter Town name: ");
        String townName = bufferedReader.readLine();

        Town town = entityManager.createQuery("select t from Town t Where t.name = :townName", Town.class)
                .setParameter("townName", townName)
                .getSingleResult();

        int affectedRows = removeAdressesOfChosenTownId(town.getId());
        entityManager.getTransaction().begin();
        entityManager.remove(town);
        entityManager.getTransaction().commit();
        System.out.println(affectedRows + " Addresses in " + townName + " removed!");
    }

    private int removeAdressesOfChosenTownId(Integer id) {
        List<Address> addresses = entityManager.createQuery("select a from Address a where a.town.id = :id")
                        .setParameter("id", id)
                        .getResultList();

        entityManager.getTransaction().begin();

        entityManager.createQuery("update Employee e set e.address=null WHERE e.address IN :addresses")
                .setParameter("addresses", addresses)
                .executeUpdate();
        entityManager.getTransaction().commit();

        entityManager.getTransaction().begin();
        addresses.forEach(entityManager::remove);
        entityManager.getTransaction().commit();
        return addresses.size();
    }

    @SuppressWarnings("unchecked")
    private void exTwelve() {
      List<Object[]> rows = entityManager.createNativeQuery("SELECT d.name, MAX(e.salary) AS `m_salary` FROM departments as d JOIN employees e on d.department_id = e.department_id\n" +
              "GROUP BY d.name\n" +
              "HAVING `m_salary` not between 30000 and 70000;").getResultList();

      rows.forEach(row -> System.out.println(row[0] + " $" + row[1]));


    }

    private void exEleven() throws IOException {
        String pattern = bufferedReader.readLine();
        List<Employee> employees = entityManager.createQuery("select e from Employee e where e.firstName like :pattern", Employee.class)
                .setParameter("pattern", pattern + "%")
                .getResultList();
        employees.forEach(employee -> System.out.println(employee.getFirstName() + " " + employee.getLastName() + " - $" + employee.getSalary()));
    }

    private void exTen() {
        entityManager.getTransaction().begin();
        Query query = entityManager.createQuery("UPDATE Employee e set e.salary = e.salary*1.12 where e.department.id in :ids")
                .setParameter("ids", Set.of(1,2,4,11));
        query.executeUpdate();
        entityManager.getTransaction().commit();
        List<Employee>employees = entityManager.createQuery("SELECT e from Employee e where e.department.id in :ids1", Employee.class)
                .setParameter("ids1", Set.of(1,2,4,11))
                .getResultList();
        employees.forEach(employee -> System.out.println(employee.getFirstName() + " " + employee.getLastName() + " ($" + employee.getSalary() + ")"));
    }

    private void exNine() {
        entityManager.createQuery("Select p from Project p order by p.name", Project.class)
                .setMaxResults(10)
                .getResultList()
                .forEach(project -> System.out.println("Project name: " + project.getName() + "\n    Project Description: " +  project.getDescription() + "\n    Project Start Date: " + project.getStartDate() + "\n    Project End Date: " + project.getEndDate()));
    }

    private void exEight() throws IOException {
        System.out.println("Enter id:");
        int id = Integer.parseInt(bufferedReader.readLine());
        Employee employee = entityManager.createQuery("select e from Employee e where e.id = :id_num", Employee.class)
                .setParameter("id_num", id)
                .getSingleResult();
        System.out.printf("%s %s - %s%n", employee.getFirstName(), employee.getLastName(), employee.getJobTitle());
        employee.getProjects().forEach(project -> System.out.printf("   %s%n", project.getName()));
    }

    private void exSeven() {
       entityManager.createQuery("select a from Address a order by a.employees.size DESC", Address.class)
               .setMaxResults(10)
               .getResultList()
               .forEach(address -> System.out.printf("%s, %s - %d employees%n", address.getText(), address.getTown() == null?"Unknown":address.getTown().getName(), address.getEmployees().size()));
    }

    private void exTwo(){
        entityManager.getTransaction().begin();
        Query query = entityManager.createQuery("update Town t set t.name = upper(t.name) where length(t.name)>=5");
        int affectedRows = query.executeUpdate();
        entityManager.getTransaction().commit();
        System.out.println(affectedRows);
    }

    private void exThree() throws IOException {
        System.out.println("Enter employee full name: ");
        String[] fullName = bufferedReader.readLine().split(" ");
        String firstName = fullName[0];
        String lastName = fullName[1];
        double countOfFoundEmployees = entityManager.createQuery("select count(e) FROM Employee e where e.firstName = :f_name AND e.lastName = :l_name", Long.class)
                .setParameter("f_name", firstName)
                .setParameter("l_name", lastName)
                .getSingleResult();
        if (countOfFoundEmployees<=0) System.out.println("Could not find the employee in the database!");
        else System.out.println("Employee found");

    }

    private void exFour(){
         entityManager.createQuery("select e FROM Employee e where e.salary>:min_salary", Employee.class)
                .setParameter("min_salary", BigDecimal.valueOf(50000L))
                .getResultList().stream().forEach(employee -> System.out.println(employee.getFirstName() + " " + employee.getLastName()));

    }

    private void exFive(){
        entityManager.createQuery("select e From Employee e where e.department.name = :d_name ORDER BY e.salary ASC, e.id ASC ", Employee.class)
                .setParameter("d_name", "Research and Development")
                .getResultList()
                .stream()
                .forEach(employee -> System.out.println(employee.getFirstName() + " "
                        + employee.getLastName() + " from "
                        + employee.getDepartment().getName() + " - $"
                        + employee.getSalary()));
    }
    private void exSix() throws IOException {
        System.out.println("Enter employee last name:");
        String lastName = bufferedReader.readLine();
        Employee employee = entityManager.createQuery("select e FROM Employee e Where e.lastName = :l_name", Employee.class)
                .setParameter("l_name", lastName)
                .getSingleResult();

        Address address = createAddress("Vitoshka 15");
        entityManager.getTransaction().begin();
        employee.setAddress(address);
        entityManager.getTransaction().commit();
    }

    private Address createAddress(String addressText) {
        Address address = new Address();
        address.setText(addressText);
        entityManager.getTransaction().begin();
        entityManager.persist(address);
        entityManager.getTransaction().commit();
        return address;
    }
}
