/*******************************************************
*                                                     *
*  -------------------------------------------------  *
*  |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  *
*  -------------------------------------------------  *
*  |     0     |     8     |    16     |     24    |  *
*  -------------------------------------------------  *
*  |    R6     |    R7     |    R8     |    R9     |  *
*  -------------------------------------------------  *
*  -------------------------------------------------  *
*  |  8  |  9  |  10 |  11 |  12 |  13 |  14 |  15 |  *
*  -------------------------------------------------  *
*  |     32    |    40     |     48    |     56    |  *
*  -------------------------------------------------  *
*  |    R10    |    R11    |     R12   |     R13   |  *
*  -------------------------------------------------  *
*  -------------------------------------------------  *
*  |  16 |  17 |  18 |  19 |  20 |  21 |  22 |  23 |  *
*  -------------------------------------------------  *
*  |     64    |    72     |     80    |     88    |  *
*  -------------------------------------------------  *
*  |   R14/LR  |    R15    |     F1    |     F3    |  *
*  -------------------------------------------------  *
*  -------------------------------------------------  *
*  |  24 |  25 |  26 |  27 |  28 | 29  |           |  *
*  -------------------------------------------------  *
*  |     96    |    104    |    112    |    120    |  *
*  -------------------------------------------------  *
*  |    F5     |    F7     |     PC    |           |  *
*  -------------------------------------------------  *
* *****************************************************/

.file  "ontop_s390x_sysv_elf_gas.S"
.text
.align  4 # According to the sample code in the ELF ABI docs
.global ontop_fcontext
.type   ontop_fcontext, @function

#define GR_OFFSET	0
#define LR_OFFSET	64
#define SP_OFFSET	72
#define FP_OFFSET	80
#define PC_OFFSET	112
#define L_CTX		120

ontop_fcontext:
    
    # Reserved the space for stack to store the data of current context
    # before we jump to the new context.
    aghi %r15,-L_CTX

    # save the registers to the stack
    stmg %r6, %r15, GR_OFFSET(%r15)

    # save the floating point registers
    std  %f0,FP_OFFSET(%r15)
    std  %f3,FP_OFFSET+8(%r15)
    std  %f5,FP_OFFSET+16(%r15)
    std  %f7,FP_OFFSET+24(%r15)
    # Save LR as PC
    stg  %r14,PC_OFFSET(%r15)

    # Store the SP pointing to the old context-data into R0
    lgr  %r0,%r15

    # Get the SP pointing to the new context-data
    # Note: Since the return type of the jump_fcontext is struct whose
    # size is more than 8. The compiler automatically passes the 
    # address of the transfer_t where the data needs to store into R2.

    # Hence the first param passed to the jump_fcontext which represent
    # the fctx we want to switch to is present in R3
    # R2 --> Address of the return transfer_t struct
    # R3 --> Context we want to switch to
    # R4 --> Data
    lgr  %r15,%r3

    # Load the registers with the data present in context-data of the
    # context we are going to switch to
    lmg  %r6,%r15,GR_OFFSET(%r15)

    # Restore Floating point registers
    ld	 %f1,FP_OFFSET(%r15)
    ld	 %f3,FP_OFFSET+8(%r15)
    ld	 %f5,FP_OFFSET+16(%r15)
    ld	 %f7,FP_OFFSET+24(%r15)

    # Skip PC

    # Adjust the stack
    aghi %r15,L_CTX

    # R2 --> Address where the return transfer_t is stored
    # R0 --> FCTX
    # R4 --> DATA
    # R5 --> Context function

    # Store the elements to return transfer_t
    stg  %r15, 0(%r2)
    stg  %r4, 8(%r2)

    # Note: The address in R2 points to the place where the return
    # transfer_t is stored. Since context_function take transfer_t
    # as first parameter. And R2 is the register which holds the
    # first parameter value.

    #jump to context function
    br 	%r5

.size   ontop_fcontext,.-ontop_fcontext
# Mark that we don't need executable stack.
.section .note.GNU-stack,"",%progbits
