package co.inter.piggies.support;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

/**
 * Placeholder entity: Hibernate refuses to build a {@code SessionFactory} for a persistence
 * unit with no entities, so the application context cannot start without at least one.
 *
 * <p>Delete this once the application has entities of its own in {@code src/main}.
 */
@Entity
public class Piggy {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
