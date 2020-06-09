package org.devzone.webclient.model;

import lombok.*;
import lombok.experimental.Accessors;

@Data
@Getter
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class Address  {

    private String postalCode;
    private String locality;
    @EqualsAndHashCode.Exclude
    private String state;

}
