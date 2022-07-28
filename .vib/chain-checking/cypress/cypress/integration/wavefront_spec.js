describe('chain api test', () => {
    it('GET-list user', () => {
        cy.request({
            method: 'GET',
            url: '/api/v2/proxy?offset=0&limit=100',
            headers: {
                Authorization: `Bearer ${Cypress.env('token')}`,
            }
        }).then((response) => {
            expect(response.status).equal(200)
            expect(response.body.response.items.map(i => i.name)).to.include('Proxy on proxy-dge')
        })
    })
})